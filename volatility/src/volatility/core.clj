(ns volatility.core
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
            [volatility.email :refer [completed-email]]
            [volatility.weather :refer [weather-report]]))


(def cred {:access-key (:access-key env)
           :secret-key (:secret-key env)
           :endpoint   "us-east-1"
           :fred (:fred env)
           :bucket-name (:bucket-name env)
           :key (:key env)})


(def fred-key (:fred cred))


(defn perc-chng [[t t-minus1]]
  (let [perc-chng (* 100
                     (/ (- (:value t) (:value t-minus1))
                        (:value t-minus1)))]
    (assoc t :perc-chng perc-chng)))

(defn get-data [series-id title]
  (let [start-date
        (->> (t/minus (t/now) (t/years 1))
             (f/unparse (f/formatter "yyyy-MM-dd")))
        data (-> (str "https://api.stlouisfed.org/fred/series/observations"
                      "?series_id=" series-id "&file_type=json&sort_order="
                      "desc&observation_start="start-date"&api_key="
                      fred-key)
                 slurp
                 (json/read-str :key-fn keyword)
                 :observations)]
    (->> data
         (filter #(not= (:value %) "."))
         (map #(update % :value read-string))
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
  (println (str "uploading file for volatility"))
  (let [file-name "volatility.json"
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


(defn execute-volatility! []
  (let [vix-data
        (map #(assoc % :url "https://finance.yahoo.com/quote/%5Evix?ltr=1")
             (get-data "VIXCLS" "CBOE Volatility Index: VIX"))
        vix-big (big vix-data "vol big")
        vix-consec (consec vix-data "vol consec")

        pol-data
        (map #(assoc % :url "http://www.policyuncertainty.com/")
             (get-data "USEPUINDXD"
                       "Economic Policy Uncertainty Index for United States"))
        pol-big (big pol-data "vol big")
        pol-consec (consec pol-data "vol consec")]

    (->> (concat vix-big vix-consec pol-big pol-consec
                 (weather-report))
         upload-file-s3)))

(defn -handleRequest
  [this input output context]
  (time (do (execute-volatility!)
            (completed-email cred))))
