(ns example-standalone-webservice.main
  (:require [crux.api :as api]
            [crux.io :as crux-io]
            [clojure.instant :as instant]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [yada.yada :refer [handler listener]]
            [hiccup2.core :refer [html]]
            [hiccup.util]
            [yada.resource :refer [resource]]
            [yada.resources.classpath-resource]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [example-standalone-webservice.backup :as backup])
  (:import [crux.api IndexVersionOutOfSyncException]
           java.io.Closeable
           [java.util Calendar Date TimeZone UUID]
           java.time.ZoneId
           java.text.SimpleDateFormat))

(defn- format-date [^Date d]
  (when d
    (.format ^SimpleDateFormat (.get ^ThreadLocal @#'instant/thread-local-utc-date-format) d)))

(defn- pp-with-date-links [tt-str x]
  (hiccup.util/raw-string
   (str/replace (with-out-str
                  (pp/pprint x))
                #"\#inst \"(.+)\""
                (fn [[s d]]
                  (str/replace s d (str "<a href=\"/?vt=" d (when tt-str
                                                              (str "&tt=" tt-str)) "\">" d "</a>"))))))

(defn- page-head [title]
  [:head
   [:meta {:charset "utf-8"}]
   [:meta {:http-equiv "Content-Language" :content "en"}]
   [:meta {:name "google" :content "notranslate"}]
   [:link {:rel "stylesheet" :type "text/css" :href "/static/styles/normalize.css"}]
   [:link {:rel "stylesheet" :type "text/css" :href "/static/styles/main.css"}]
   [:title title]])

(defn- footer []
  [:footer
   "© 2019 "
   [:a {:href "https://juxt.pro"} "JUXT"]])

(defn- status-block [crux]
  [:div.status
   [:h4 "Status:"]
   [:pre (pp-with-date-links nil (api/status crux))]])

(defn- valid-time-link [vt]
  (let [vt-str (format-date vt)]
    [:a {:href (str "/?vt=" vt-str)} vt-str]))

(defn- draw-timeline-graph [tx-log min-time max-time now max-known-tt vt tt width height]
  (let [known-times (for [{:crux.tx/keys [tx-time tx-ops]} tx-log
                          command tx-ops
                          x command
                          :when (inst? x)]
                      {:vt x :tt tx-time})
        time-diff (double (- (inst-ms max-time) (inst-ms min-time)))
        min-time-str (format-date min-time)
        max-time-str (format-date max-time)
        now-str (format-date now)
        time->x (fn [t]
                  (* width (/ (- (inst-ms t) (inst-ms min-time)) time-diff)))
        onclick-timeline-js "window.location = ('/?%s=' + Math.round(%f * (window.event.offsetX / window.event.target.getBoundingClientRect().width) + %d) + '&%s=%d');"]
    [:svg.timeline-graph {:version "1.1" :xmlns "http://www.w3.org/2000/svg" :viewBox (str "0 0 " width " " height)}
     [:a {:href (str "/?vt=" min-time-str)} [:text.min-time {:x 0 :y (* 0.55 height)} min-time-str]]
     [:a {:href (str "/?vt=" max-time-str)} [:text.max-time {:x width :y (* 0.55 height)} max-time-str]]
     (when max-known-tt
       [:a.max-transaction-time {:href (str "/?tt=" (format-date max-known-tt))}
        [:text {:x (* 0.995 (time->x max-known-tt)) :y (* 0.85 height)} "MAX"]
        [:line {:x1 (time->x max-known-tt) :y1 (* 0.25 height) :x2 (time->x max-known-tt) :y2 (* 0.75 height)}]])
     [:g.axis
      [:text.axis-name {:x 0 :y (* 0.2 height)} "VT: "
       [:tspan.axis-value (format-date vt)]]
      [:line.axis-line {:x1 0 :y1 (* 0.25 height) :x2 width :y2 (* 0.25 height) :stroke-width (* 0.01 height)
                        :onclick (format onclick-timeline-js "vt" time-diff (inst-ms min-time) "tt" (inst-ms tt))}]
      (for [{:keys [tt vt]} (sort-by :vt known-times)
            :let [vt-str (format-date vt)
                  x (time->x vt)]]
        [:a.timepoint {:href (str "/?vt=" vt-str "&tt=" (format-date tt))}
         [:g
          [:line.timepoint-marker {:x1 x :y1 (* 0.2 height) :x2 x :y2 (* 0.3 height)}]
          [:text {:x x :y (* 0.15 height)} (str vt-str " | " (format-date tt))]]])]
     [:g.axis
      [:line.axis-line {:x1 0 :y1 (* 0.75 height) :x2 width :y2 (* 0.75 height) :stroke-width (* 0.01 height)
                        :onclick (format onclick-timeline-js "tt" time-diff (inst-ms min-time) "vt" (inst-ms vt))}]
      (for [tt (sort (map :tt known-times))
            :let [tt-str (format-date tt)
                  x (time->x tt)]]
        [:a.timepoint {:href (str "/?tt=" tt-str)}
         [:g
          [:line.timepoint-marker {:x1 x :y1 (* 0.7 height) :x2 x :y2 (* 0.8 height)}]
          [:text {:x x :y (* 0.65 height)} tt-str]]])
      [:text.axis-name {:x 0 :y (* 0.9 height)} "TT: "
       [:tspan.axis-value (or (format-date tt) "empty")]]]
     [:a.time-horizon {:href "/"}
      [:text {:x (* 1.005 (time->x now)) :y (* 0.85 height)} "NOW"]
      [:line {:x1 (time->x now) :y1 (* 0.1 height) :x2 (time->x now) :y2 (* 0.9 height)}]]
     (when tt
       [:line.bitemp-coordinates {:x1 (time->x vt) :y1 (* 0.25 height) :x2 (time->x tt) :y2 (* 0.75 height)}])]))

(defn- parse-query-date [d]
  (if (re-find #"^\d+$" d)
    (Date. (Long/parseLong d))
    (instant/read-instant-date d)))

(defn- min-max-time [^Date from]
  (let [utc (ZoneId/of "UTC")
        ld (.toLocalDate (.atZone (.toInstant from) utc))]
    [(Date/from (.toInstant (.atStartOfDay (.minusDays ld 9) utc)))
     (Date/from (.toInstant (.atStartOfDay (.plusDays ld 1) utc)))]))

(defn index-handler
  [ctx {:keys [crux]}]
  (fn [ctx]
    (let [{:strs [vt tt]} (get-in ctx [:parameters :query])
          now (Date.)
          vt (if (not (str/blank? vt))
               (parse-query-date vt)
               now)
          tt (when (not (str/blank? tt))
               (parse-query-date tt))
          tx-log (with-open [tx-log-cxt (api/new-tx-log-context crux)]
                   (vec (api/tx-log crux tx-log-cxt nil true)))
          max-known-tt (:crux.tx/tx-time (last tx-log))
          tt (or tt max-known-tt)
          tt (if (pos? (compare tt max-known-tt))
               max-known-tt
               tt)
          db (api/db crux vt tt)
          [min-time max-time] (min-max-time now)
          edit-comment-oninput-js "this.style.height = ''; this.style.height = this.scrollHeight + 'px';"]
      (str
       "<!DOCTYPE html>"
       (html
        [:html {:lang "en"}
         (page-head "Message Board")
         [:body
          [:header
           [:h2 [:a {:href "/"} "Message Board"]]]
          [:div.timetravel
           (draw-timeline-graph tx-log min-time max-time now max-known-tt vt tt 750 100)]
          [:div.comments
           [:h3 "Comments"]
           [:ul
            (for [[message name created id]
                  (->> (api/q db
                              '{:find [m n c e]
                                :where [[e :message-post/message m]
                                        [e :message-post/name n]
                                        [e :message-post/created c]]})
                       (sort-by #(nth % 2)))]
              [:li.comment
               [:span.comment-meta
                name
                " at "
                (valid-time-link created)
                (let [history (with-open [snapshot ^Closeable (api/new-snapshot db)]
                                (mapv :crux.db/valid-time (api/history-descending db snapshot id)))
                      history-onchange-js "this.form.submit();"]
                  (when (> (count history) 1)
                    [:span " • "
                     [:form.version-history {:method "GET" :action "/"  :autocomplete "off"}
                      [:label {:for "version-history-list"} "edited at "]
                      [:select#version-history-list {:name "vt" :onchange history-onchange-js :placeholder "versions:"}
                       (for [history-vt history
                             :let [vt-str (format-date history-vt)]]
                         [:option {:value vt-str :selected (= vt history-vt)} vt-str])]]]))]
               [:form.edit-comment {:action (str "/comment/" id) :method "POST" :autocomplete "off"}
                [:fieldset
                 [:input {:type "hidden" :name "created" :value (format-date created)}]
                 [:input {:type "hidden" :name "name" :value name}]
                 [:textarea {:id (str "edit-message-" id) :rows (count (str/split-lines message)) :name "message" :required true
                             :oninput edit-comment-oninput-js} message]]
                [:div.buttons
                 [:input.primary {:type "submit" :name "_action" :value "Edit"}]
                 [:input {:type "submit" :name "_action" :value "Delete"}]
                 [:input {:type "submit" :name "_action" :value "Delete History"}]
                 [:input {:type "submit" :name "_action" :value "Evict"}]]]])]

           [:div.add-comment-box
            [:h5 "Add new comment"]
            [:form {:action "/comment" :method "POST" :autocomplete "off"}
             [:fieldset
              [:input {:type "hidden" :name "created" :value (format-date vt)}]
              [:input {:type "text" :id "name" :name "name" :required true :placeholder "Name"}]
              [:textarea {:cols "40" :rows "10" :id "message" :name "message" :required true :placeholder "Message"}]]
             [:input.primary {:type "submit" :name "_action" :value "Comment"}]
             [:input.primary {:type "submit" :name "_action" :value "Bitemporal Comment"}]]]]

          [:div
           [:a {:href "tx-log"} "Transaction History"]]
          (status-block crux)
          (footer)]])))))

(defn tx-log-handler [ctx {:keys [crux]}]
  (fn [ctx]
    (let [tx-log (with-open [tx-log-cxt (api/new-tx-log-context crux)]
                   (vec (api/tx-log crux tx-log-cxt nil true)))]
      (str
       "<!DOCTYPE html>"
       (html
        [:html
         (page-head "Message Board - Transaction History")
         [:body
          [:header
           [:h2 [:a {:href ""} "Transaction History"]
            [:small "(Earliest first.)"]]]
          [:div.transaction-history
           [:table
            [:thead
             [:th (str :crux.tx/tx-id)]
             [:th (str :crux.tx/tx-time)]
             [:th (str :crux.tx/tx-ops)]]
            [:tbody
             (for [{:crux.tx/keys [tx-id tx-time tx-ops]} tx-log
                   :let [tx-time-str (format-date tx-time)]]
               [:tr
                [:td tx-id]
                [:td [:a {:href (str "/?tt=" tx-time-str)} tx-time-str]]
                [:td (pp-with-date-links (format-date tx-time) tx-ops)]])]]]
          [:div
           [:a {:href "/"} "Back to Message Board"]]
          (status-block crux)
          (footer)]])))))

(defn redirect-with-time [ctx valid-time transaction-time]
  (assoc (:response ctx)
         :status 302
         :headers {"location" (str "/?" "vt=" (format-date valid-time) "&tt=" (format-date transaction-time))}))

(defn post-comment-handler
  [ctx {:keys [crux]}]
  (let [{:keys [name message created _action]} (get-in ctx [:parameters :form])]
    (let [id (UUID/randomUUID)
          created (case (str/lower-case _action)
                    "comment" (Date.)
                    "bitemporal comment" (instant/read-instant-date created))
          {:keys [crux.tx/tx-time]}
          (api/submit-tx
           crux
           [[:crux.tx/put
             id
             {:crux.db/id id
              :message-post/created created
              :message-post/name name
              :message-post/message message}
             created]])]
      (redirect-with-time ctx created tx-time))))

(defn edit-comment-handler
  [ctx {:keys [crux]}]
  (let [{:keys [name message created _action]} (get-in ctx [:parameters :form])
        id (UUID/fromString (get-in ctx [:parameters :path :id]))
        now (Date.)
        {:keys [crux.tx/tx-time]}
        (case (str/lower-case _action)
          "delete"
          (api/submit-tx
           crux
           [[:crux.tx/delete
             id
             now]])
          "delete history"
          (api/submit-tx
           crux
           [[:crux.tx/delete
             id
             (instant/read-instant-date created)
             now]])
          "evict"
          (api/submit-tx
           crux
           [[:crux.tx/evict
             id
             (instant/read-instant-date created)
             now]])
          "edit"
          (api/submit-tx
           crux
           [[:crux.tx/put
             id
             {:crux.db/id id
              :message-post/created (instant/read-instant-date created)
              :message-post/edited now
              :message-post/name name
              :message-post/message message}
             now]]))]
    (redirect-with-time ctx now tx-time)))

(defn application-resource
  [system]
  ["/"
   [[""
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(index-handler % system)}}})]
    ["tx-log"
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(tx-log-handler % system)}}})]
    ["comment"
     (resource
      {:methods
       {:post {:consumes "application/x-www-form-urlencoded"
               :parameters {:form {:created String
                                   :name String
                                   :message String
                                   :_action String}}
               :produces "text/html"
               :response #(post-comment-handler % system)}}})]

    [["comment/" :id]
     (resource
      {:methods
       {:post {:consumes "application/x-www-form-urlencoded"
               :parameters {:form {:created String
                                   :name String
                                   :message String
                                   :_action String}}
               :produces "text/html"
               :response #(edit-comment-handler % system)}}})]
    ["static"
     (yada.resources.classpath-resource/new-classpath-resource "static")]]])

(def index-dir "data/db-dir-1")
(def log-dir "data/eventlog-1")

(def crux-options
  {:kv-backend "crux.kv.rocksdb.RocksKv"
   :bootstrap-servers "kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092"
   :event-log-dir log-dir
   :db-dir index-dir
   :server-port 8080})

(def backup-options
  {:backend :shell
   :backup-dir "data/backup"
   :shell/backup-script "bin/backup.sh"
   :shell/restore-script "bin/restore.sh"})

(defn run-system [{:keys [server-port] :as options} with-system-fn]
  (with-open [crux-system (case (System/getenv "CRUX_MODE")
                            "LOCAL_NODE" (api/start-local-node options)
                            (api/start-standalone-system options))
              http-server
              (let [l (listener
                       (application-resource {:crux crux-system})
                       {:port server-port})]
                (log/info "started webserver on port:" server-port)
                (reify Closeable
                  (close [_]
                    ((:close l)))))]
    (with-system-fn crux-system)))

(defn -main []
  (try
    (backup/check-and-restore crux-options backup-options)
    (run-system
     crux-options
     (fn [crux-system]
       (while (not (.isInterrupted (Thread/currentThread)))
         (Thread/sleep (* 1000 60 60 1)) ;; every hour
         (backup/backup-current-version crux-system crux-options backup-options))))
    (catch IndexVersionOutOfSyncException e
      (crux-io/delete-dir index-dir)
      (-main))
    (catch Exception e
      (log/error e "what happened" (ex-data e)))))

(comment
  (def s (future
           (run-system
            crux-options
            (fn [_]
              (def crux)
              (Thread/sleep Long/MAX_VALUE)))))
  (future-cancel s))
