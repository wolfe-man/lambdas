(ns meetings.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clj-time.predicates :as pr]
            [clj-time.periodic :as p]
            [environ.core :refer [env]]
            [meetings.fomc :refer [fomc-speaks]]
            [meetings.email :refer [completed-email]]
            [meetings.world :refer [execute-world-events]]))

(def fomc-dates
  ["2016-11-02" "2016-12-14" "2017-02-01" "2017-03-15" "2017-05-03"
   "2017-06-14" "2017-07-26" "2017-09-20" "2017-11-01" "2017-12-13"])

(def bank-of-england-dates ["2017-02-02" "2017-03-16" "2017-05-11" "2017-06-15"
                            "2017-08-03" "2017-09-14" "2017-11-02" "2017-12-14"])

(def ecb-dates ["2017-01-19" "2017-03-09" "2017-04-27" "2017-06-08" "2017-07-20"
                "2017-09-07" "2017-10-26" "2017-12-14"])

(def boj-dates ["2017-01-31" "2017-03-16" "2017-04-27" "2017-06-16" "2017-07-20"
                "2017-09-21" "2017-10-31" "2017-12-21"])

(def opec-monthly-report ["2017-01-18" "2017-02-13" "2017-03-14" "2017-05-11"
                          "2017-06-13" "2017-07-12" "2017-08-10" "2017-09-12"
                          "2017-10-11" "2017-11-13" "2017-12-13"])

(def bank-of-england-inflation-report
  ["2016-05-12" "2016-08-04" "2016-11-03" "2017-02-02"
   "2017-05-11" "2017-08-03" "2017-11-02"])

(def imf-world-economic-outlook-dates
  ["2016-10-04" "2017-04-18" "2017-10-10"])

(def beige-book-dates
  ["2016-11-30" "2017-01-18" "2017-03-01" "2017-04-19"
   "2017-05-31" "2017-07-12" "2017-09-06" "2017-10-18" "2017-11-29"])

(def market-holiday-dates
  ["2017-02-20" "2017-04-14" "2017-05-29" "2017-07-04" "2017-09-04"
   "2017-11-23" "2017-12-25"])

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
  (println "uploading file for meetings")
  (let [file-name (str "meetings" ".json")
        file (format "/tmp/%s" file-name)]
    (->> json-data
         json/write-str
         (spit file))
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (str (:key cred) file-name)
                   :file (io/file file))))

(defn build-dates! [dates title style url fn]
  (->> dates
       (map #(hash-map :date (if fn (fn %) %)
                       :title title
                       :class style
                       :url url))))

(defn week-dates [day]
  (map #(f/unparse (f/formatter "yyyy-MM-dd") %)
       (filter day (p/periodic-seq (t/minus (t/now) (t/months 3))
                                  (t/plus (t/now) (t/months 3))
                                  (t/days 1)))))

(defn execute-meetings! []
  (upload-file-s3
   (concat
    #_(execute-world-events)
    (fomc-speaks)
    (build-dates! fomc-dates "US Federal Reserve (FOMC) Press Release"
                  "meeting" "https://www.federalreserve.gov/monetarypolicy.htm"
                  nil)
    (build-dates! fomc-dates "US Federal Reserve (FOMC) Minutes"
                  "meeting" "https://www.federalreserve.gov/monetarypolicy.htm"
                  (fn [x] (-> (f/formatter "yyyy-MM-dd")
                              (f/parse x)
                              (t/plus (t/days 21))
                              c/to-long)))
    (build-dates! (week-dates pr/friday?) "Commitments of Traders (COT) Report"
                  "projections" "http://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"
                  nil)
    (build-dates! (week-dates pr/wednesday?) "Weekly Petroleum Status (EIA) Report"
                  "projections" "https://www.eia.gov/petroleum/supply/weekly/"
                  nil)
    (build-dates! beige-book-dates
                  "US Federal Reserve (FOMC) Beige Book"
                  "meeting" "https://www.federalreserve.gov/monetarypolicy/beige-book-default.htm"
                  nil)
    (build-dates! bank-of-england-dates
                  "Bank Of England (BOE) Monetary Policy Committee Minutes"
                  "meeting" "http://www.bankofengland.co.uk/publications/minutes/Pages/mpc/default.aspx"
                  nil)
    (build-dates! bank-of-england-inflation-report
                  "Bank Of England (BOE) Inflation Report"
                  "meeting" "http://www.bankofengland.co.uk/publications/Pages/inflationreport/default.aspx"
                  nil)
    (build-dates! ecb-dates
                  "European Central Bank (ECB) Monetary Policy Decisions"
                  "meeting" "https://www.ecb.europa.eu/press/govcdec/mopo/2017/html/index.en.html"
                  nil)
    (build-dates! boj-dates
                  "Bank of Japan (BOJ) Monetary Policy Meeting Minutes"
                  "meeting" "https://www.boj.or.jp/en/mopo/mpmsche_minu/index.htm/"
                  nil)
    (build-dates! imf-world-economic-outlook-dates
                  "International Monetary Fund (IMF) World Economic Outlook"
                  "meeting" "http://www.imf.org/en/publications/weo"
                  nil)
    (build-dates! opec-monthly-report
                  "Organization of the Petroleum Exporting Countries (OPEC) Monthly Oil Market Report"
                  "meeting" "http://www.opec.org/opec_web/en/publications/338.htm"
                  nil)
    (build-dates! market-holiday-dates
                  "Market Holiday"
                  "holiday" "https://www.marketbeat.com/stock-market-holidays/"
                  nil))))


(defn -handleRequest
  [this input output context]
  (time (do (execute-meetings!)
            (completed-email cred))))
