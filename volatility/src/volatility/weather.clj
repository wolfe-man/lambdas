(ns volatility.weather
  (:require [clojure.data.json :as json]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.coerce :as c]))


(defn national-disasters [date]
  (-> (str "https://www.fema.gov/api/open/v1/DisasterDeclarationsSummaries"
           "?$format=json&$orderby=lastRefresh%20desc"
           "&$filter=declarationDate%20gt%20%27" date "%27")
      slurp
      (json/read-str :key-fn keyword)
      :DisasterDeclarationsSummaries))


(defn weather-report []
  (->> (t/minus (t/now) (t/years 1))
       (f/unparse (f/formatter "yyyy-MM-dd"))
       national-disasters
       (filter #(and (= "FL" (:state %))
                     (= "Hurricane" (:incidentType %))))
       (map #(clojure.set/rename-keys % {:incidentBeginDate :date}))
       (map #(update % :date c/to-date-time))
       (map #(update % :date (partial f/unparse (f/formatter "yyyy-MM-dd"))))
       (map #(select-keys % [:date :title]))
       (map #(assoc % :url (str "https://www.fema.gov/disasters"
                                "?field_state_tid_selective=47"
                                "&field_disaster_type_term_tid=6840"
                                "&field_disaster_declaration_type_value=All"
                                "&items_per_page=20")))
       (map #(assoc % :class "vol"))
       set))
