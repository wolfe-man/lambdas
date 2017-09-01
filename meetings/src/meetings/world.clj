(ns meetings.world
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [net.cgrand.enlive-html :as html]))


(defn flat [s]
  (if (coll? s)
    (first (:content s))
    s))


(defn combine [page [title url]]
  {:title (flat (first (:content title)))
   :date (->> (first (:content (first (:content url))))
              (#(clojure.string/split % #" "))
              (take 3)
              (apply format "%s %s %s")
              (f/parse (f/formatter "MMMM dd, yyyy") )
              (f/unparse (f/formatter "yyyy-MM-dd")))
   :class "meeting"
   :url page})


(defn world-events [month year]
  (let [page (str "http://www.cfr.org/publication/"
                  "world_events_calendar.html?month=" month
                  "&year=" year)
        html (with-open [inputstream
                         (-> page
                             java.net.URL.
                             .openConnection
                             (doto (.setRequestProperty "User-Agent"
                                                        "Mozilla/5.0 ..."))
                             .getContent)]
               (html/html-resource inputstream))]
    (->> (html/select html [:article.spotlight :header])
         (map #((comp (partial combine page) :content) %)))))


(defn execute-world-events []
  (concat (world-events (t/month (t/now)) (t/year (t/now)))
          (world-events (inc (t/month (t/now))) (t/year (t/now)))))


;;https://fidelity-fcm.econoday.com/byweek.asp?cust=fidelity-global
