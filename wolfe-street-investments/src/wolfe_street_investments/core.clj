(ns wolfe-street-investments.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.periodic :as p]
            [clj-time.predicates :as pr]
            [environ.core :refer [env]]))

(def my-time (c/to-string (c/to-timestamp (t/now))))

(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :bucket-name (:bucket-name env)
           :consume-key (:consume-key env)
           :write-key (:write-key env)})


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn consume-file-s3 []
  (let [s3-response (with-aws-credentials cred
                      (s3/get-object :bucket-name (:bucket-name cred)
                                     :key (:consume-key cred)))]
    (-> s3-response
        :object-content
        slurp
        (json/read-str :key-fn keyword)
        :result)))


(defn generate-text [coll pl1 n pl2]
  (let [cnt-str (format "there %s %s event%s. " pl1 n pl2)
        events-list (->> coll
                         (map :title)
                         (map #(clojure.string/replace % #"\([^)]*\)" ""))
                         (map #(clojure.string/replace % #"Continuous Contract" ""))
                         (map #(clojure.string/replace % #"Longer Run" ""))
                         (map #(clojure.string/replace % #"Summary of Economic" ""))
                         (map #(clojure.string/replace % #", Central Tendency, Midpoint" ""))
                         (map #(clojure.string/replace % #"S&P/" ""))
                         (map #(clojure.string/replace % #"S&P" "S and P"))
                         (map #(clojure.string/replace % #"©" ""))
                         (map #(clojure.string/replace % #"#1" ""))
                         (map #(clojure.string/replace % #"   " " "))
                         (map #(clojure.string/replace % #"  " " "))
                         (interleave (range 1 100))
                         (interpose ". ")
                         (apply str))]
    (str cnt-str events-list)))


(defn create-day-speech [[[a yesterday] [b today] [c tomorrow]
                          [d day-after] [e day-after-after]]]
  [{:uid (.toString (java.util.UUID/randomUUID))
    :updateDate my-time
    :titleText (format "%s, Financial Calendar" a)
    :mainText
    (format "%s, %s" a
            (if (empty? yesterday)
              "there were not any events."
              (let [n (count yesterday)]
                (generate-text yesterday
                               (if (> n 1) "were" "was")
                               n
                               (if (> n 1) "s" "")))))}
   {:uid (.toString (java.util.UUID/randomUUID))
    :updateDate my-time
    :titleText (format "Today, Financial Calendar")
    :mainText (format "Today, %s"
                      (if (empty? today)
                        "there are not any events."
                        (let [n (count today)]
                          (generate-text today
                                         (if (> n 1) "are" "is")
                                         n
                                         (if (> n 1) "s" "")))))}
   {:uid (.toString (java.util.UUID/randomUUID))
    :updateDate my-time
    :titleText (format "%s, Financial Calendar" c)
    :mainText (format "On %s, %s" c
                      (if (empty? tomorrow)
                        "there are not any events."
                        (let [n (count tomorrow)]
                          (generate-text tomorrow
                                         (if (> n 1) "are" "is")
                                         n
                                         (if (> n 1) "s" "")))))}
   {:uid (.toString (java.util.UUID/randomUUID))
    :updateDate my-time
    :titleText (format "%s, Financial Calendar" d)
    :mainText (format "On %s, %s" d
                      (if (empty? day-after)
                        "there are not any events."
                        (let [n (count day-after)]
                          (generate-text day-after
                                         (if (> n 1) "are" "is")
                                         n
                                         (if (> n 1) "s" "")))))}
   {:uid (.toString (java.util.UUID/randomUUID))
    :updateDate my-time
    :titleText (format "%s, Financial Calendar" e)
    :mainText (format "On %s, %s" e
                      (if (empty? day-after-after)
                        "there are not any events."
                        (let [n (count day-after-after)]
                          (generate-text day-after-after
                                         (if (> n 1) "are" "is")
                                         n
                                         (if (> n 1) "s" "")))))}])


(defn weekday [n]
  (case n
    1 "Monday"
    2 "Tuesday"
    3 "Wednesday"
    4 "Thursday"
    5 "Friday"))


(defn enrich-data []
  (letfn [(date-parser [{:keys [date]}]
            (if (string? date)
              (f/parse (f/formatter "yyyy-MM-dd") date)
              (c/from-long date)))]
    (let [data (->> (consume-file-s3)
                    (map #(assoc % :date (date-parser %)))
                    (remove #(pr/weekend? (:date %))))
          past (remove pr/weekend?
                       (p/periodic-seq (t/minus (t/now) (t/days 3))
                                       (t/minus (t/now) (t/days 1))
                                       (t/days 1)))
          future (remove pr/weekend?
                         (p/periodic-seq (t/now)
                                         (t/plus (t/now) (t/days 6))
                                         (t/days 1)))
          five-dates (flatten (concat (take-last 1 past)
                                      (take 4 future)))]
      (create-day-speech
       (for [d five-dates]
         [(weekday (t/day-of-week d))
          (filter #(t/within?
                    (t/interval (t/minus d (t/days 1)) d)
                    (:date %)) data)])))))


(defn upload-file-s3 [json-data]
  (let [file-name "/tmp/flashbriefing.json"]
    (spit file-name json-data)
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (:write-key cred)
                   :file (io/file file-name))))


(defn execute []
  (->> (enrich-data)
       json/write-str
       upload-file-s3))


(defn -handleRequest
  [this input output context]
  (time (doall (execute))))
