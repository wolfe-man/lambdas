(ns offerings.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [environ.core :refer [env]]
            [offerings.ipo :refer [execute-ipo]]
            [offerings.email :refer [completed-email]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :bucket-name (:bucket-name env)
           :key (:key env)})


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn upload-file-s3 [json-data]
  (println "uploading file for offerings")
  (let [file-name (str "offerings" ".json")
        file (format "/tmp/%s" file-name)]
    (->> json-data
         json/write-str
         (spit file))
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (str (:key cred) file-name)
                   :file (io/file file))))


(defn execute-offerings! []
  (upload-file-s3
   (execute-ipo)))


(defn -handleRequest
  [this input output context]
  (time (do (execute-offerings!)
            (completed-email cred))))
