(ns aggregator.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [environ.core :refer [env]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :bucket-name (:bucket-name env)
           :prefix (:prefix env)
           :key (:key env)
           :file-name (:file-name env)})


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn list-files-s3 []
  (let [s3-response (with-aws-credentials cred
                      (s3/list-objects :bucket-name (:bucket-name cred)
                                       :prefix (:prefix cred)))]
    (->> s3-response
         :object-summaries
         (map :key)
         (filter #(re-find #".json" %))
         (remove #(re-find #".json.php" %)))))


(defn consume-file-s3 [s3-obj-key]
  (let [s3-response (with-aws-credentials cred
                      (s3/get-object :bucket-name (:bucket-name cred)
                                     :key s3-obj-key))]
    (-> s3-response
        :object-content
        slurp
        (json/read-str :key-fn keyword))))


(defn transform-date [{:keys [date]}]
  (let [day-offset 80000000
        date (if (string? date)
               (f/parse (f/formatter "yyyy-MM-dd") date)
               date)]
    (->> date
         c/to-long
         (+ day-offset)
         str)))


(defn transform-file [series-data]
  (->> series-data
       (map #(assoc % :start (transform-date %)))
       (map #(assoc % :end (transform-date %)))))


(defn enrich-data [series]
  (->> series
       consume-file-s3
       transform-file))


(defn configure-events-json-php [events]
  {:success 1
   :result events})


(defn upload-file-s3 [json-data]
  (let [file-name (:file-name cred)]
    (spit file-name json-data)
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (:key cred)
                   :file (io/file file-name))))

(defn execute []
  (let [all-files (list-files-s3)]
    (->> all-files
         (map enrich-data)
         flatten
         configure-events-json-php
         json/write-str
         upload-file-s3)))


(defn -handleRequest
  [this input output context]
  (time (doall (execute))))
