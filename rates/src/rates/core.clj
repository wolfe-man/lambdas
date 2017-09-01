(ns rates.core
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
            [rates.email :refer [completed-email]]
            [rates.fedfunds :refer [project]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :bucket-name (:bucket-name env)
           :key (:key env)
           :quandl (:quandl env)})


(def quandl-api-key (:quandl cred))


(defn perc-chng [[t t-minus1]]
  (let [z (:value t-minus1)
        perc-chng (if (zero? z)
                    0
                    (* 100
                       (/ (- (:value t) (:value t-minus1))
                          z)))]
    (assoc t :perc-chng perc-chng)))


(defn get-data [data-set value title]
  (let [date (->> (t/minus (t/now) (t/years 1))
                  (f/unparse (f/formatter "yyyy-MM-dd")))
        dataset (-> (str "https://www.quandl.com/api/v3/datasets/"
                         data-set ".json?" "api_key=" quandl-api-key
                         "&start_date="date)
                    slurp
                    (json/read-str :key-fn keyword)
                    :dataset)
        column-names(->> (:column_names dataset)
                         (map #(clojure.string/replace % #"[ ]" "") )
                         (map keyword))
        yields (:data dataset)]
    (->> yields
         (map #(merge (zipmap column-names %)))
         (map #(select-keys % [:Date (keyword value) :Settle]))
         (map #(clojure.set/rename-keys % {(keyword value) :value
                                           :Date :date
                                           :Settle :value}))
         (remove #(= 0.0 (:value %)))
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
  (println (str "uploading file for rates"))
  (let [file-name "rates.json"
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


(defn execute-rates! []
  (let [data-10yr
        (map #(assoc % :url "https://finance.yahoo.com/bonds")
             (get-data "USTREASURY/YIELD" "10YR" "10 Year US Treasury Bond Rate"))
        big-10yr (big data-10yr "rate big")
        consec-10yr (consec data-10yr "rate consec")

        data-3mo
        (map #(assoc % :url "https://finance.yahoo.com/bonds")
             (get-data "USTREASURY/YIELD" "3MO" "3 Month US Treasury Bond Rate"))
        big-3mo (big data-3mo "rate big")
        consec-3mo (consec data-3mo "rate consec")

        data-10yr-tips
        (map #(assoc % :url "https://finance.yahoo.com/bonds")
             (get-data "USTREASURY/REALYIELD" "10YR"
                       "10 Year US Treasury Inflation Protected Securities Rate"))
        big-10yr-tips (big data-10yr-tips "rate big")
        consec-10yr-tips (consec data-10yr-tips "rate consec")]
    #_(data-ff-30day
     (map #(assoc % :url (str "http://www.cmegroup.com/trading/"
                              "interest-rates/stir/30-day-federal-fund.html"))
          (get-data "CHRIS/CME_FF1" "10YR"
                    (str "30 Day Federal Funds Futures, Continuous "
                         "Contract #1 (FF1) (Front Month)")))
     big-ff-30day (big data-ff-30day "rate big")
     consec-ff-30day (consec data-ff-30day "rate consec"))
    (->> (concat big-10yr consec-10yr big-3mo consec-3mo
                 big-10yr-tips consec-10yr-tips
                 ;;big-ff-30day consec-ff-30day
                 (project))
         upload-file-s3)))

(defn -handleRequest
  [this input output context]
  (time (do (execute-rates!)
            (completed-email cred))))
