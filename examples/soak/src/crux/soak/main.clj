(ns crux.soak.main
  (:require [ring.util.response :as resp]
            [ring.middleware.params :as params]
            [bidi.ring]
            [ring.adapter.jetty :as jetty]
            [crux.api :as api]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.set :as set]
            [hiccup2.core :refer [html]]
            [integrant.core :as ig]
            [integrant.repl :as ir]
            [clojure.tools.logging :as log])
  (:import java.io.Closeable
           java.util.Date
           java.time.Duration
           java.text.SimpleDateFormat
           crux.api.NodeOutOfSyncException
           org.eclipse.jetty.server.Server))


(def soak-secrets
  (-> (System/getenv "SOAK_SECRETS")
      (json/decode)))

(def kafka-properties-map
  {"ssl.endpoint.identification.algorithm" "https"
   "sasl.mechanism" "PLAIN"
   "sasl.jaas.config" (str (->> ["org.apache.kafka.common.security.plain.PlainLoginModule"
                                 "required"
                                 (format "username=\"%s\"" (get soak-secrets "CONFLUENT_API_TOKEN"))
                                 (format "password=\"%s\"" (get soak-secrets "CONFLUENT_API_SECRET"))]
                                (string/join " "))
                           ";")
   "security.protocol" "SASL_SSL"})

(defn wrap-node [handler node]
  (fn [request]
    (handler (assoc request :crux-node node))))

(defn get-current-weather [node location valid-time]
  (->> (keyword (str location "-current"))
       (api/entity (api/db node valid-time))))

(defn get-weather-forecast [node location valid-time]
  (let [past-five-days (map
                        #(Date/from (-> (.toInstant valid-time)
                                        (.minus (Duration/ofHours 1))
                                        (.minus (Duration/ofDays %))))
                        (range 0 5))]
    (remove
     nil?
     (map
      (fn [transaction-time]
        (try
          [transaction-time
           (api/entity
            (api/db node valid-time transaction-time)
            (keyword (str location "-forecast")))]
          (catch NodeOutOfSyncException e
            nil)))
      past-five-days))))

(def date-formatter (SimpleDateFormat. "dd/MM/yyyy"))

(defn filter-weather-map [{:keys [main wind weather]}]
  (let [filtered-main (-> (set/rename-keys main {:temp :temperature})
                          (select-keys [:temperature :feels_like :pressure :humidity]))]
    (when-not (empty? filtered-main)
      (-> filtered-main
          (assoc :wind_speed (:speed wind))
          (assoc :current_weather (:description (first weather)))))))

(defn render-weather-map [weather-map]
  (reduce
   (fn [elems key]
     (conj elems [:p
                  [:b (string/upper-case (string/replace (str (name key) ": ") #"_" " "))]
                  (key weather-map)]))
   []
   (keys weather-map)))

(defn weather-handler [req]
  (let [location-id (get-in req [:query-params "Location"])
        date (some-> (get-in req [:query-params "Date"]) (Long/parseLong) (Date.))
        node (:crux-node req)]
    (resp/response
     (str
      (html
       [:header
        [:link {:rel "stylesheet" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]]
       [:body
        [:style "body { display: inline-flex; font-family: Helvetica }
                 #location { text-align: center; width: 300px; padding-right: 30px; }
                 #current-weather { width: 300px; padding-right: 30px; }
                 #forecasts { display: inline-flex; }
                 #forecast { padding-right: 10px; }
                 #forecast h3 { margin-top: 0px; } "]
        [:div#location
         [:h1 (-> (api/entity (api/db node) (keyword (str location-id "-current")))
                  (:location-name))]
         [:form {:action "/weather.html"}
          [:input {:type "hidden" :name "Location" :value location-id}]
          (into [:select {:name "Date"}]
                (map
                 (fn [next-date]
                   (let [formatted-date (.format date-formatter next-date)]
                     [:option
                      {:value (.getTime next-date)
                       :selected (when (= formatted-date (.format date-formatter date)) "selected")}
                      formatted-date]))
                 (map
                  #(Date/from (-> (.toInstant (Date.))
                                  (.plus (Duration/ofDays %))))
                  (range 0 5))))
          [:p [:input {:type "submit" :value "Select Date"}]]]]
        (when-let [current-weather (get-current-weather node location-id date)]
          [:div#current-weather
           [:h2 "Current Weather"]
           (into
            [:div#weather]
            (->> current-weather
                 filter-weather-map
                 render-weather-map))])
        [:div#weather-forecast
         [:h2 "Forecast History"]
         (into
          [:div#forecasts]
          (map
           (fn [[forecast-date forecast]]
             (into
              [:div#forecast
               [:h3 (.format date-formatter forecast-date)]]
              (->> forecast
                   filter-weather-map
                   render-weather-map)))
           (get-weather-forecast node location-id date)))]])))))

(defn homepage-handler [req]
  (let [node (:crux-node req)]
    (resp/response
     (str
      (html
       [:header
        [:link {:rel "stylesheet" :type "text/css" :href "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.css"}]]
       [:body
        [:div#homepage
         [:style "#homepage { text-align: center; font-family: Helvetica; }
                h1 { font-size: 3em; }
                h2 { font-size: 2em; }"]
         [:h1 "Crux Weather Service"]
         [:h2 "Locations"]
         [:form {:target "_blank" :action "/weather.html"}
          [:input {:type "hidden" :name "Date" :value (System/currentTimeMillis)}]
          [:p
           (into [:select {:name "Location"}]
                 (map
                  (fn [[location-name]] [:option {:value (-> (string/replace location-name #" " "-")
                                                                      (string/lower-case))}
                                                  location-name])
                  (api/q (api/db node) '{:find [l]
                                         :where [[e :location-name l]]})))]
          [:p [:input {:type "submit" :value "View Location"}]]]]])))))

(def bidi-handler
  (bidi.ring/make-handler ["" [["/" {"index.html" homepage-handler
                                     "weather.html" weather-handler}]
                               [true (bidi.ring/->Redirect 307 homepage-handler)]]]))

(defmethod ig/init-key :soak/crux-node [_ node-opts]
  (let [node (api/start-node node-opts)]
    (log/info "Loading Weather Data...")
    (api/sync node)
    (log/info "Weather Data Loaded!")
    node))

(defmethod ig/halt-key! :soak/crux-node [_ ^Closeable node]
  (.close node))

(defmethod ig/init-key :soak/jetty-server [_ {:keys [crux-node server-opts]}]
  (log/info "Starting jetty server...")
  (jetty/run-jetty (-> bidi-handler
                       (params/wrap-params)
                       (wrap-node crux-node))
                   server-opts))

(defmethod ig/halt-key! :soak/jetty-server [_ ^Server server]
  (.stop server))

(def config
  {:soak/crux-node {:crux.node/topology '[crux.kafka/topology crux.kv.rocksdb/kv-store]
                    :crux.kafka/bootstrap-servers (get soak-secrets "CONFLUENT_BROKER")
                    :crux.kafka/tx-topic "soak-transaction-log"
                    :crux.kafka/doc-topic "soak-docs"
                    :crux.kafka/kafka-properties-map kafka-properties-map}
   :soak/jetty-server {:crux-node (ig/ref :soak/crux-node)
                       :server-opts {:port 8080
                                     :join? false}}})

(defn -main [& args]
  (ir/set-prep! (fn [] config))
  (ir/go))
