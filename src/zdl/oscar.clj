(ns zdl.oscar
  (:require
   [clojure.core.async :as a]
   [clojure.java.io :as io]
   [clojure.java.process :as p]
   [clojure.string :as str]
   [gremid.xml :as gx]
   [gremid.xml-schema :as gxs]
   [hato.client :as hc]
   [jsonista.core :as json]
   [taoensso.telemere :as tm]
   [tick.core :as t])
  (:import
   (com.codahale.metrics MetricRegistry Slf4jReporter)
   (java.io ByteArrayInputStream ByteArrayOutputStream)
   (java.net URI)
   (java.nio.charset Charset)
   (java.util.concurrent TimeUnit)
   (java.util.concurrent.atomic AtomicLong)
   (java.util.zip ZipEntry ZipOutputStream)
   (org.apache.commons.compress.compressors.zstandard ZstdCompressorInputStream)))

(set! *warn-on-reflection* true)

(def base-url
  "https://oscar-prive.huma-num.fr/2301/de_meta/")

(defn getenv
  [k]
  (or (System/getenv k)
      (throw (IllegalStateException. (str "$" k " not defined")))))

(def user
  (getenv "OSCAR_USER"))

(def password
  (getenv "OSCAR_PASSWORD"))

(defn http-get
  [filename]
  (-> {:method      :get
       :url         (str base-url filename)
       :http-client {:ssl-context {:insecure? true}}
       :basic-auth  {:user user :pass password}
       :as          :stream}
      (hc/request)
      (get :body)))

(defn chunk-sort-key
  [jsonl-file]
  (->> jsonl-file (re-seq #"\d+") (first) (parse-long)))

(defrecord Chunk [filename])

(def chunks
  (delay
    (with-open [input (io/reader (http-get "checksum.sha256"))]
      (->>
       (line-seq input)
       (map (comp last #(str/split % #"\s+")))
       (sort-by chunk-sort-key)
       (map ->Chunk)
       (vec)))))

(extend-protocol io/IOFactory
  Chunk
  (make-input-stream [this opts]
    (io/make-input-stream (http-get (:filename this)) opts)))

(def download-dir
  (doto (io/file "data" "corpus") (.mkdirs)))

(defn download
  [& _]
  (doseq [f (map :filename @chunks)]
    (p/exec {:dir download-dir}
            "curl" "-C" "-" "-O" "-u" (str user ":" password)
            (str base-url f))))

(defn downloaded-chunk
  [{:keys [filename]}]
  (io/file download-dir filename))

(defn valid-document?
  [{{quality-warnings "quality_warnings"} "metadata" content "content"}]
  (and (nil? quality-warnings)   (some-> content str/trim not-empty)))

(defn ws-offsets
  ([s]
   (ws-offsets s (re-matcher #"\s+" s)))
  ([s ^java.util.regex.Matcher m]
   (when (.find m) (lazy-seq (cons (.start m) (ws-offsets s m))))))

(def incipit-max-length
  50)

(defn incipit
  [content]
  (let [len (count content)
        end (or
             (last (take-while #(< % incipit-max-length) (ws-offsets content)))
             len)
        ss  (subs content 0 end)]
    (cond-> ss (nil? (re-find #"\p{Punct}$" ss)) (str "…"))))

(defn url->domain
  [s]
  (when-let [host (. (URI. s) (getHost))]
    (when-not (re-matches #"^[0-9\.]+$" host)
      (->> (str/split host #"\.")
           (take-last 2)
           (str/join \.)))))

(def corpus-title
  "Open Super-large Crawled Aggregated coRpus (OSCAR) v23.01.")

(defn convert-document
  [{{date "warc-date" url "warc-target-uri"} "warc_headers"
    content                                  "content"
    :as                                      doc}]
  (let [domain     (url->domain url)
        title      (cond->> (incipit content) domain (str domain ": "))
        title-stmt [:titleStmt [:title {:type "main"} title]]
        timestamp  (t/zoned-date-time date)
        date       (t/date timestamp)
        paragraphs (str/split content #"[\r\n]+")
        xml        (ByteArrayOutputStream.)]
    (try
      (->>
       (gx/sexp->node
        [:TEI {:xmlns "http://www.tei-c.org/ns/1.0"}
         [:teiHeader
          [:fileDesc
           title-stmt
           [:publicationStmt [:p]]
           [:sourceDesc
            [:bibl (str title " " corpus-title " " (t/format "dd.MM.YYYY" date) ".")]
            [:biblFull
             title-stmt
             [:publicationStmt
              [:publisher corpus-title]
              [:ptr {:type "URL" :target url}]
              [:date (str date)]
              [:availability {:n "OR0W" :status "restricted"}
               [:p "Ohne Rechte, Kontextanzeige max. 0 Wörter"]
               [:ab {:type "access" :n "I"} "Nur intern."]]]]]]
          [:profileDesc
           [:creation [:date {:type "download"} (str date)]]]]
         [:text [:body [:div (for [p paragraphs] [:p p])]]]])
       (gx/node->events)
       (gx/write-events xml))
      (list {:xml        (.toByteArray xml)
             :paragraphs (count paragraphs)
             :tokens     (count (str/split content #"\s+"))})
      (catch Throwable t
        (tm/error! {:id ::convert :error t :data (dissoc doc "content")})
        nil))))

(def parse-jsonl-xf
  (comp (map json/read-value)
        (filter valid-document?)
        (mapcat convert-document)))

(defn chunk->ch
  ([c]
   (chunk->ch c (a/chan 64)))
  ([c ch]
   (a/thread
     (with-open [input (io/input-stream c)
                 input (ZstdCompressorInputStream. input)
                 input (io/reader input)]
       (a/<!! (a/onto-chan!! ch (sequence parse-jsonl-xf (line-seq input))))))
   ch))

(defn metrics-xf
  [rf]
  (let [registry (MetricRegistry.)
        reporter (doto (.build (Slf4jReporter/forRegistry registry))
                   (.start 30 TimeUnit/SECONDS))
        dm       (.meter registry "documents")
        pm       (.meter registry "paragraphs")
        tm       (.meter registry "tokens")]
    (fn
      ([] (rf))
      ([result]
       (.close reporter)
       (rf result))
      ([result {:keys [paragraphs tokens] :as doc}]
       (.mark dm 1)
       (.mark pm paragraphs)
       (.mark tm tokens)
       (rf result doc)))))

(defn tokens-total-xf
  [rf]
  (let [total (AtomicLong. 0)]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result {:keys [tokens] :as doc}]
       (rf result (assoc doc :tokens-total (.addAndGet total tokens)))))))

(defn assoc-output-coords
  [n {:keys [tokens-total] :as doc}]
  (assoc doc
         :chunk (quot tokens-total 1000000000)
         :path (str/join \/ [(format "%03d" (quot n 1000000))
                             (format "%03d" (mod (quot n 1000) 1000))
                             (format "%09d.tei.xml" n)])))

(def tei-all-schema
  (->>
   (URI. "https://www.tei-c.org/release/xml/tei/custom/schema/relaxng/tei_all.rng")
   (gxs/->rng-schema)))

(defn assert-tei-valid
  [{:keys [xml] :as doc}]
  (when-let [errors (seq (gxs/rng-validate tei-all-schema (ByteArrayInputStream. xml)))]
    (tm/error! {:id ::tei :data errors}))
  doc)

(def corpus-output-xf
  (comp
   #_(map assert-tei-valid)
   tokens-total-xf
   (map-indexed assoc-output-coords)
   metrics-xf
   (take 10000)))

(defn ch->seq
  [ch]
  (when-let [v (a/<!! ch)] (lazy-seq (cons v (ch->seq ch)))))

(def ^java.io.File conversion-dir
  (io/file "data" "tei"))

(defn convert
  [& _]
  (when (.exists conversion-dir)
    (run! io/delete-file (reverse (file-seq conversion-dir))))
  (.mkdirs conversion-dir)
  (let [ch     (a/merge (map (comp chunk->ch downloaded-chunk) @chunks) 64)
        docs   (sequence corpus-output-xf (ch->seq ch))
        chunks (partition-by :chunk docs)]
    (doseq [[{cn :chunk} :as chunk] chunks]
      (let [zip-file (io/file conversion-dir (format "%04d.zip" cn))]
        (with-open [zip (io/output-stream zip-file)
                    zip (ZipOutputStream. zip (Charset/forName "UTF-8"))]
          (doseq [{:keys [path xml] :as _doc} chunk]
            (. zip (putNextEntry (ZipEntry. ^String path)))
            (io/copy (ByteArrayInputStream. ^bytes xml) zip)
            (. zip (closeEntry))))))))
