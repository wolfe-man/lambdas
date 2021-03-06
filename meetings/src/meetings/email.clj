(ns meetings.email
  (:require [amazonica.aws.simpleemail :as ses]
            [environ.core :refer [env]]))


(def email (:email env))


(defn completed-email [cred]
  (ses/send-email cred
                  :destination {:to-addresses [email]}
                  :source email
                  :message {:subject "Meetings"
                            :body {:text "Meetings Done."}}))
