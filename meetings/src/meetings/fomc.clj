(ns meetings.fomc
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [net.cgrand.enlive-html :as html]))


(defn fomc-speaks []
  (let [html (-> (str "https://www.stlouisfed.org/fomcspeak")
                 java.net.URL.
                 html/html-resource)
        urls (->> (html/select html [:div.rightSideInner [:a (html/attr? :href)]])
                  (map #((comp :href :attrs) %))
                  butlast)
        title (->> (html/select html [:div.rightSideInner [:a (html/attr? :href)]])
                   (map :content)
                   butlast
                   flatten
                   (map clojure.string/trim))
        dates (->> (html/select html [:strong])
                   (map :content)
                   (map first)
                   rest
                   (drop-last 2)
                   (map #(f/parse (f/formatter "MMM. dd, yyyy") %))
                   (map #(f/unparse (f/formatter "yyyy-MM-dd") %)))]
    (map #(hash-map :url %1 :date %2 :title %3 :class "speech") urls dates title)))
