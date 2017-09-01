(ns market.core
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
            [market.email :refer [completed-email]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :quandl (:quandl env)
           :bucket-name (:bucket-name env)
           :key (:key env)})


(def quandl-api-key (:quandl cred))


(defn perc-chng [[t t-minus1]]
  (let [perc-chng (* 100
                     (/ (- (:settle t) (:settle t-minus1))
                        (:settle t-minus1)))]
    (assoc t :perc-chng perc-chng)))


(defn get-data [code]
  (let [start-date (t/minus (t/now) (t/years 1))
        {:keys [name column_names data]}
        (-> (str "https://www.quandl.com/api/v3/datasets/"  code
                 ".json?api_key=" quandl-api-key "&start_date=" start-date)
            slurp
            (json/read-str :key-fn keyword)
            :dataset)
        column-names-fmt
        (->> column_names
             (map #(clojure.string/replace % #"[ |.]" "-"))
             (map clojure.string/lower-case)
             (map keyword))]
    (->> data
         (map #(merge {:title name} (zipmap column-names-fmt %)))
         (map #(clojure.set/rename-keys % {:settlement-price :settle,
                                           :rate :settle}))
         (partition 2 1)
         (map perc-chng))))


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
  (println (str "uploading file for market"))
  (let [file-name "market.json"
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
  (let [dow-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5EDJI?p=^DJI")
             (get-data "CHRIS/CME_YM1"))
        dow-big (big dow-data "index big")
        dow-consec (consec dow-data "index consec")

        sp-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5EGSPC?p=^GSPC")
             (get-data "CHRIS/CME_SP1"))
        sp-big (big sp-data "index big")
        sp-consec (consec sp-data "index consec")

        nasdaq-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5EIXIC?p=^IXIC")
             (get-data "CHRIS/CME_NQ1"))
        nasdaq-big (big nasdaq-data "index big")
        nasdaq-consec (consec nasdaq-data "index consec")

        russell-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5ERUT")
             (get-data "CHRIS/ICE_TF1"))
        russell-big (big russell-data "index big")
        russell-consec (consec russell-data "index consec")

        ftse-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5EFTSE/?p=^FTSE")
             (get-data "CHRIS/LIFFE_Z1"))
        ftse-big (big ftse-data "index big")
        ftse-consec (consec ftse-data "index consec")

        stoxx-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5ESTOXX50E/?p=^STOXX50E")
             (get-data "CHRIS/EUREX_FESX1"))
        stoxx-big (big stoxx-data "index big")
        stoxx-consec (consec stoxx-data "index consec")

        dax-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5EGDAXI/?p=^GDAXI")
             (get-data "CHRIS/EUREX_FDAX1"))
        dax-big (big dax-data "index big")
        dax-consec (consec dax-data "index consec")

        ;;hang-seng-data (get-data "CHRIS/HKEX_HSI1")
        nikkei-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5EN225/?p=^N225")
             (get-data "CHRIS/CME_NK1"))
        nikkei-big (big nikkei-data "index big")
        nikkei-consec (consec nikkei-data "index consec")

        wti-data
        (map #(assoc % :url "https://www.bloomberg.com/quote/CL1:COM")
             (get-data "CHRIS/ICE_T1"))
        wti-big (big wti-data "commodity big")
        wti-consec (consec wti-data "commodity consec")

        brent-data
        (map #(assoc % :url "https://www.bloomberg.com/quote/CO1:COM")
             (get-data "CHRIS/ICE_B1"))
        brent-big (big brent-data "commodity big")
        brent-consec (consec brent-data "commodity consec")

        gold-data
        (map #(assoc % :url "https://www.bloomberg.com/markets/commodities/futures/metals")
             (get-data "CHRIS/CME_GC1"))
        gold-big (big gold-data "commodity big")
        gold-consec (consec gold-data "commodity consec")

        silver-data
        (map #(assoc % :url "https://www.bloomberg.com/markets/commodities/futures/metals")
             (get-data "CHRIS/CME_SI1"))
        silver-big (big silver-data "commodity big")
        silver-consec (consec silver-data "commodity consec")

        jpy-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdjpy=x?ltr=1")
             (get-data "CUR/JPY"))
        jpy-big (big jpy-data "fx big")
        jpy-consec (consec jpy-data "fx consec")

        gbp-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdgbp=x?ltr=1")
             (get-data "CUR/GBP"))
        gbp-big (big gbp-data "fx big")
        gbp-consec (consec gbp-data "fx consec")

        eur-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdeur=x?ltr=1")
             (get-data "CUR/EUR"))
        eur-big (big eur-data "fx big")
        eur-consec (consec eur-data "fx consec")

        cny-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdcny=x?ltr=1")
             (get-data "CUR/CNY"))
        cny-big (big cny-data "fx big")
        cny-consec (consec cny-data "fx consec")

        mxn-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdmxn=x?ltr=1")
             (get-data "CUR/MXN"))
        mxn-big (big mxn-data "ex big")
        mxn-consec (consec mxn-data "ex consec")

        inr-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdinr=x?ltr=1")
             (get-data "CUR/INR"))
        inr-big (big inr-data "ex big")
        inr-consec (consec inr-data "ex consec")

        hkd-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/usdhkd=x?ltr=1")
             (get-data "CUR/HKD"))
        hkd-big (big hkd-data "ex big")
        hkd-consec (consec hkd-data "ex consec")]

    (->> (concat sp-big sp-consec nasdaq-big nasdaq-consec
                 dow-big dow-consec russell-big russell-consec
                 ftse-big ftse-consec stoxx-big stoxx-consec
                 dax-big dax-consec nikkei-big nikkei-consec
                 wti-big wti-consec brent-big brent-consec
                 gold-big gold-consec silver-big silver-consec
                 jpy-big jpy-consec gbp-big gbp-consec
                 eur-big eur-consec cny-big cny-consec
                 mxn-big mxn-consec inr-big inr-consec
                 hkd-big hkd-consec)
         upload-file-s3)))

(defn -handleRequest
  [this input output context]
  (time (do (execute-market!)
            (completed-email cred))))
