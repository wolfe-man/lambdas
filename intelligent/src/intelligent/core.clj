(ns intelligent.core
  (:gen-class
   :implements [com.amazonaws.services.lambda.runtime.RequestStreamHandler])
  (:require [amazonica.aws.s3 :as s3]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [environ.core :refer [env]]
            [intelligent.tickers :refer [tickers]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :bucket-name (:bucket-name env)
           :key (:key env)})


(defmacro retry
  "Evaluates expr up to cnt + 1 times, retrying if an exception
  is thrown. If an exception is thrown on the final attempt, it
  is allowed to bubble up."
  [cnt expr]
  (letfn [(go [cnt]
              (if (zero? cnt)
                expr
                `(try ~expr
                      (catch Exception e#
                        (retry ~(dec cnt) ~expr)))))]
    (go cnt)))


(defn get-stock-valuation [tics]
  (retry 3
   (do (Thread/sleep 1000)
       (->> tics
            (map #(str "%22" % "%22"))
            (interpose "%2C%20")
            clojure.string/join
            (#(str "https://query.yahooapis.com/v1/public/yql"
                   "?q=select%20*%20from%20yahoo.finance.quotes%20"
                   "where%20symbol%20in%20"
                   "(" %
                   ")%20and%20%20MarketCapitalization%20like%20%22%25B%25%22%20"
                   "and%20DividendShare%20!%3D%20%220.00%22%20and%20Currency"
                   "%20%3D%20%22USD%22&format=json&diagnostics=true&env="
                   "store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback="))
            slurp
            (#(json/read-str % :key-fn keyword))
            ((comp :quote :results :query))))))


(defn non-valid-mapping? [{:keys [Symbol AverageDailyVolume
                                  FiftydayMovingAverage
                                  TwoHundreddayMovingAverage
                                  Open
                                  ShortRatio BookValue
                                  DividendYield PEGRatio]}]
  (some nil? [Symbol AverageDailyVolume
              Open
              ShortRatio BookValue
              DividendYield PEGRatio]))


(defn avg-mil-volume? [{:keys [AverageDailyVolume]}]
  (some->> (read-string AverageDailyVolume)
           (< 1000000)))


(defn book-gt-bid? [{:keys [BookValue Open]}]
  (some->> (read-string Open)
           (> (read-string BookValue))))


(defn nil-short-ratio? [{:keys [ShortRatio]}]
  (some->> ShortRatio
           read-string
           nil?))


(defn golden-cross? [{:keys [TwoHundreddayMovingAverage
                             FiftydayMovingAverage]}]
  (some->> (read-string TwoHundreddayMovingAverage)
           (> (read-string FiftydayMovingAverage))))


(defn pos-pegratio-less-than-1? [{:keys [PEGRatio]}]
  (or (zero? (read-string PEGRatio))
      (and (pos? (read-string PEGRatio))
           (> 1 (read-string PEGRatio)))))


(defn prep-quotes [tics]
  (->> (flatten tics)
       (partition-all 50)
       (map get-stock-valuation) ;;has dividend and B market cap and USD
       flatten
       (remove empty?)))


(defn execute-valuation [qualified-tickers]
  (->> qualified-tickers
       (filter book-gt-bid?)
       (filter golden-cross?)
       (filter pos-pegratio-less-than-1?)
       (remove nil-short-ratio?)
       (remove nil?)
       (sort-by #(read-string (:ShortRatio %)))
       (map #(select-keys % [:Symbol :AverageDailyVolume
                             :TwoHundreddayMovingAverage
                             :FiftydayMovingAverage
                             :ShortRatio :BookValue
                             :DividendYield :PEGRatio]))))


(defn execute-qualification []
  (->> tickers
       prep-quotes
       (remove non-valid-mapping?)
       (filter avg-mil-volume?)))


(defmacro with-aws-credentials
  [credentials [aws-func & args]]
  `(let [updated-args# (if (and (:access-key ~credentials)
                                (:secret-key ~credentials))
                         (cons ~credentials (list ~@args))
                         (list ~@args))]
     (apply ~aws-func updated-args#)))


(defn upload-file-s3 [json-data]
  (println (str "uploading file for stocks"))
  (let [file-name (format "stocks%s.json" (t/month (t/now)))
        file (format "/tmp/%s" file-name)]
    (->> json-data
         json/write-str
         (spit file))
    (s3/put-object cred
                   :bucket-name (:bucket-name cred)
                   :key (str (:key cred) file-name)
                   :file (io/file file))))


(defn create-title [symbol two-day-avg fifty-day-avg short-ratio
                    bookvalue dividend]
  (format (str "%s is recommended to watch (200 day avg: %s, 50 day avg: "
               "%s, book value: %s, dividend: %s, short ratio: %s)")
          symbol two-day-avg fifty-day-avg
          bookvalue dividend short-ratio))


(defn enrich-obj [{:keys [Symbol AverageDailyVolume TwoHundreddayMovingAverage
                          FiftydayMovingAverage ShortRatio BookValue DividendYield
                          PEGRatio]}]
  {:title (create-title Symbol TwoHundreddayMovingAverage FiftydayMovingAverage
                        ShortRatio BookValue DividendYield)
   :class "stocks"
   :date (f/unparse (f/formatter "yyyy-MM-dd") (t/now))
   :url (str "https://finance.yahoo.com/quote/" Symbol)})


(defn execute-intelligent! []
  (let [qualified (execute-qualification)
        valuation (execute-valuation qualified)]
    (->> valuation
         (map enrich-obj)
         upload-file-s3)))

(defn -handleRequest
  [this input output context]
  (time (execute-intelligent!)))
