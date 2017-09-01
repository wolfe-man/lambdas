(ns rates.fedfunds
  (:require [clojure.data.json :as json]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [environ.core :refer [env]]))


(def fred-key (:fred env))
(def quandl-api-key (:quandl env))
(def start-date (f/unparse (f/formatter "yyyy-MM-dd")
                           (t/minus (t/now) (t/years 1))))

(defn ff-rate-upper [{:keys [value]}]
  (first (drop-while #(> value %) (take 20 (iterate (partial + 0.25) 0)))))


(defn get-rate-data []
  (->> (str "https://api.stlouisfed.org/fred/series/observations"
            "?series_id=DFF&file_type=json&sort_order=desc"
            "&observation_start="start-date"&api_key="
            fred-key)
       slurp
       (#(json/read-str % :key-fn keyword))
       :observations
       (map #(update % :value read-string))
       (remove #(nil? (:value %)))))


(defn get-futures-data []
  (let [{:keys [name column_names data]}
        (-> (str "https://www.quandl.com/api/v3/datasets/CHRIS/CME_FF1"
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
         (map #(merge {:title name} (zipmap column-names-fmt %))))))


(defn prob-rate-chng [{:keys [settle value upper]}]
  (-> (- 100 settle)
      (- value)
      (/ (- upper value))
      (* 100.0)))


(defn add-title-url [{prob :prob :as hash}]
  (assoc hash
         :title (format (str "The Probability of a Federal Funds Rate "
                             "Change is %.2f%%") prob)
         :url "https://fred.stlouisfed.org/series/DFF"
         :class "rate"))


(defn project []
  (->> (get-rate-data)
       (concat (get-futures-data))
       (group-by :date)
       (remove #(> 2 (count (val %))))
       (map second)
       (map #(apply merge %))
       (map #(assoc % :upper (ff-rate-upper %)))
       (map #(assoc % :prob (prob-rate-chng %)))
       (filter #(or (> (:prob %) 100)))
       (map #(select-keys % [:date :value :settle :upper :prob]))
       (map add-title-url)))
