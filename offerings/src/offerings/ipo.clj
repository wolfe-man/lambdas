(ns offerings.ipo
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [net.cgrand.enlive-html :as html]))


(defn parse-row [url [company symbol _ _ _ offer date]]
  (if (> (count (re-seq #"," (first (:content offer)))) 2)
    {:date (->> (first (:content date))
                (f/parse (f/formatter "MM/dd/yyyy"))
                (f/unparse (f/formatter "yyyy-MM-dd")))
     :title (str (first (:content (first (:content company)))) " ("
                 (first (:content (first (:content symbol))))
                 ") Initial Public Offering")
     :class "offerings"
     :url url}))


(defn ipo [url]
  (Thread/sleep 500)
  (let [html (-> url
                 java.net.URL.
                 html/html-resource)
        rows (->> (html/select html [:div.genTable :tr])
                  (map :content)
                  rest
                  (map #(filter coll? %)))]
    (->> rows
         (map #(parse-row url %))
         (remove nil?))))


(defn execute-ipo []
  (let [upcoming (ipo (str "http://www.nasdaq.com/markets/ipos/"
                           "activity.aspx?tab=upcoming"))
        past-months (->> (t/now)
                         (iterate #(t/minus % (t/months 1)))
                         (take 6)
                         (map #(f/unparse (f/formatter "yyyy-MM") %)))
        pricings (map #(ipo (str "http://www.nasdaq.com/markets/ipos/"
                                 "activity.aspx?tab=pricings&month=" %))
                      past-months)]
    (->> (concat pricings upcoming)
         flatten
         (remove empty?))))
