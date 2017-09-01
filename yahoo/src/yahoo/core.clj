(ns yahoo.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [environ.core :refer [env]]
            [incanter.stats :refer [mean sd]]
            [yahoo.email :refer [completed-email]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :bucket-name (:bucket-name env)
           :key (:key env)})


(defn perc-chng [[t t-minus1]]
  (let [perc-chng (* 100
                     (/ (- (:settle t) (:settle t-minus1))
                        (:settle t-minus1)))]
    (assoc t :perc-chng perc-chng)))


(defn get-ticker-data [tic title]
  (let [start-date (f/unparse (f/formatter "yyyy-MM-dd")
                              (t/minus (t/now) (t/years 1)))
        end-date (f/unparse (f/formatter "yyyy-MM-dd") (t/now))]
    (->> tic
         (#(str "https://query.yahooapis.com/v1/public/yql?"
                "q=select%20*%20from%20yahoo.finance.historicaldata%20"
                "where%20symbol%20%3D%20%22" % "%22%20"
                "and%20startDate%20%3D%20%22" start-date
                "%22%20and%20endDate%20%3D%20%22" end-date
                "%22&format=json&diagnostics=true&env=store%3A%2F%2F"
                "datatables.org%2Falltableswithkeys&callback="))
         slurp
         (#(json/read-str % :key-fn keyword))
         ((comp :quote :results :query))
         (map #(clojure.set/rename-keys % {:Symbol :symbol
                                           :Date :date
                                           :Close :settle}))
         (map #(update % :settle read-string))
         (partition 2 1)
         (map perc-chng)
         (map #(assoc % :title title)))))


(defn calc-consec [{:keys [perc-chng date]} data]
  (let [chng-fn (if (pos? perc-chng) pos? neg?)]
    (->> data
         (drop-while #(t/after? (c/to-date-time (:date %)) (c/to-date-time date)))
         (take-while #(chng-fn (:perc-chng %)))
         count)))


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn upload-file-s3 [json-data]
  (println (str "uploading file for yahoo"))
  (let [file-name "yahoo.json"
        file (format "/tmp/%s" file-name)]
    (->> json-data
         json/write-str
         (spit file))
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (str (:key cred) file-name)
                   :file (io/file file))))


(defn add-class [class data]
  (map #(assoc % :class class) data))


(defn add-consec [data]
  (->> data
       (map #(assoc % :consec-days (calc-consec % data)))
       (filter #(> (:consec-days %) 3))))


(defn add-consec-title [data]
  (map #(assoc % :title ((fn [{:keys [title consec-days perc-chng]}]
                           (if (pos? perc-chng)
                             (format "%s increased for %d consecutive days"
                                     title consec-days)
                             (format "%s decreased for %d consecutive days"
                                     title consec-days))) %))
       data))


(defn consec [data class]
  (->> data
       add-consec
       add-consec-title
       (add-class class)))


(defn add-big [data]
  (let [perc-chngs (map :perc-chng data)
        pc-mean (mean perc-chngs)
        pc-sd (* 1.5 (sd perc-chngs))
        n+ (+ pc-mean pc-sd)
        n- (- pc-mean pc-sd)]
    (filter #(or (> (:perc-chng %) n+) (< (:perc-chng %) n-)) data)))


(defn add-big-title [data]
  (map #(assoc % :title ((fn [{:keys [title perc-chng]}]
                           (if (pos? perc-chng)
                             (format "%s increased %.2f%%"
                                     title perc-chng)
                             (format "%s decreased %.2f%%"
                                     title perc-chng))) %))
       data))


(defn big [data class]
  (->> data
       add-big
       add-big-title
       (add-class class)))


(defn execute-market! []
  (let [xly-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLY?p=XLY")
             (get-ticker-data "XLY" "Consumer Discret Sel Sect SPDR ETF (XLY)"))
        xly-big (big xly-data "sector big")
        xly-consec (consec xly-data "sector consec")

        xlp-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLP?p=XLP")
             (get-ticker-data "XLP" "Consumer Staples Select Sector SPDR ETF (XLP)"))
        xlp-big (big xlp-data "sector big")
        xlp-consec (consec xlp-data "sector consec")

        xle-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLE?p=XLE")
             (get-ticker-data "XLE" "Energy Select Sector SPDR ETF (XLE)"))
        xle-big (big xle-data "sector big")
        xle-consec (consec xle-data "sector consec")

        xlf-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLF?p=XLF")
             (get-ticker-data "XLF" "Financial Select Sector SPDR ETF (XLF)"))
        xlf-big (big xlf-data "sector big")
        xlf-consec (consec xlf-data "sector consec")

        xlv-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLV?p=XLV")
             (get-ticker-data "XLV" "Health Care Select Sector SPDR ETF (XLV)"))
        xlv-big (big xlv-data "sector big")
        xlv-consec (consec xlv-data "sector consec")

        xli-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLI?p=XLI")
             (get-ticker-data "XLI" "Industrial Select Sector SPDR ETF (XLI)"))
        xli-big (big xli-data "sector big")
        xli-consec (consec xli-data "sector consec")

        xlb-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLB?p=XLB")
             (get-ticker-data "XLB" "Materials Select Sector SPDR ETF (XLB)"))
        xlb-big (big xlb-data "sector big")
        xlb-consec (consec xlb-data "sector consec")

        xlre-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLRE?p=XLRE")
             (get-ticker-data "XLRE" "Real Estate Select Sector SPDR (XLRE)"))
        xlre-big (big xlre-data "sector big")
        xlre-consec (consec xlre-data "sector consec")

        xlk-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLK?p=XLK")
             (get-ticker-data "XLK" "Technology Select Sector SPDR ETF (XLK)"))
        xlk-big (big xlk-data "sector big")
        xlk-consec (consec xlk-data "sector consec")

        xlu-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/XLU?p=XLU")
             (get-ticker-data "XLU" "Utilities Select Sector SPDR ETF (XLU)"))
        xlu-big (big xlu-data "sector big")
        xlu-consec (consec xlu-data "sector consec")]

    (->> (concat xly-big xly-consec xlp-big xlp-consec
                 xle-big xle-consec xlf-big xlf-consec
                 xlv-big xlv-consec xli-big xli-consec
                 xlb-big xlb-consec xlre-big xlre-consec
                 xlk-big xlk-consec xlu-big xlu-consec)
         upload-file-s3)))

(defn -handleRequest
  [this input output context]
  (time (do (execute-market!)
            (completed-email cred))))
