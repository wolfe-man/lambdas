(ns fred.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [environ.core :refer [env]]
            [fred.email :refer [completed-email]]))

(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :fred-key (:fred-key env)
           :bucket-name (:bucket-name env)
           :key (:key env)})

(def fred-key (:fred-key cred))

(def series [;;fed projections
             "FEDTARCTMLR" "GDPC1CTMLR" "PCECTPICTMLR" "UNRATECTMLR"
             ;;inflation measures
             "MICH" "UMCSENT" "PCEPI" "CPIAUCSL" "PCEPILFE" "CPILFESL" "TCU"
             "M2V"
             ;; "T5YIFR"
             ;;output measures
             "GDPC1" "CP" "GDPDEF" "FINSLC1" "BUSINV" "NEWORDER"
             ;;unemployment measures
             "UNRATE" "CIVPART" "ICSA" "PAYEMS" "USPRIV" "AHETPI" "NPPTTL"
             ;;housing measures
             "HOUST" ;;"EXHOSLUSM495S"
             "PERMIT" "HSN1F" "SPCS20RSA"])


(defn series-type [series-info]
  (let [series (:id series-info)]
    (cond
      (some (partial = series)
            ["FEDTARCTMLR" "GDPC1CTMLR"
             "PCECTPICTMLR" "UNRATECTMLR"]) "projections"
      (some (partial = series)
            ["MICH" "T5YIFR" "UMCSENT" "PCEPI" "TCU" "M2V"
             "CPIAUCSL" "PCEPILFE" "CPILFESL"]) "inflation"
      (some (partial = series)
            ["GDPC1" "BUSINV" "CP"
             "GDPDEF" "FINSLC1" "NEWORDER"]) "gdp"
      (some (partial = series)
            ["UNRATE" "CIVPART" "ICSA"
             "PAYEMS" "USPRIV" "AHETPI" "NPPTTL"]) "unemployment"
      (some (partial = series)
            ["HOUST" "EXHOSLUSM495S" "SPCS20RSA"
             "PERMIT" "HSN1F"]) "house")))


(defn get-release-data [series-doc]
  (let [releases (-> (str "https://api.stlouisfed.org/fred/release/dates"
                          "?release_id=" (:release-id series-doc)
                          "&api_key=" fred-key "&file_type=json"
                          "&include_release_dates_with_no_data=true")
                     slurp
                     (json/read-str :key-fn keyword)
                     :release_dates)]
    (map #(merge series-doc %) releases)))


(defn get-release-id [series-id]
  (let [release (-> (str "https://api.stlouisfed.org/fred/series/release"
                         "?series_id=" series-id
                         "&api_key=" fred-key "&file_type=json")
                    slurp
                    (json/read-str :key-fn keyword)
                    :releases
                    first)]
    {:id series-id
     :release-id (:id release)
     :url (str "https://fred.stlouisfed.org/series/" series-id)}))


(defn get-series-title [{:keys [id]}]
  (-> (str "https://api.stlouisfed.org/fred/series"
           "?series_id=" id
           "&api_key=" fred-key "&file_type=json")
      slurp
      (json/read-str :key-fn keyword)
      :seriess
      first
      :title))


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn upload-file-s3 [json-data]
  (println (str "uploading file for fred" ))
  (let [file-name "fred.json"
        file (format "/tmp/%s" file-name)]
    (->> json-data
         json/write-str
         (spit file))
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (str (:key cred) file-name)
                   :file (io/file file))))


(defn filter-releases [releases]
  (let [date (t/minus (t/now) (t/years 3))
        parser (fn [x] (f/parse (f/formatter "yyyy-MM-dd") (:date x)))]
    (filter #(t/after? (parser %) date) releases)))


(defn execute-fred []
  (->> series
       (map get-release-id)
       (map #(assoc % :class (series-type %)))
       (map #(assoc % :title (get-series-title %)))
       (map get-release-data)
       flatten
       filter-releases
       upload-file-s3))

(defn execute-fred! []
  (doall (execute-fred)))

(defn -handleRequest
  [this input output context]
  (time (do (execute-fred!)
            (completed-email cred))))
