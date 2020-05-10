(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.common :as common]
   [crux.ui.subscriptions :as sub]
   [crux.ui.uikit.table :as table]
   [fork.core :as fork]
   [reagent.core :as r]
   [reitit.core :as reitit]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(defn vt-tt-inputs
  [{:keys [values handle-change handle-blur]}]
  [:div.crux-time
   [:div.input-group.valid-time
    [:div.label
     [:label "Valid Time"]]
    [:input.input {:type "date"
                   :name "valid-date"
                   :value (get values "valid-date")
                   :on-change handle-change
                   :on-blur handle-blur}]
    [:input.input {:type "time"
                   :name "valid-time"
                   :step "any"
                   :value (get values "valid-time")
                   :on-change handle-change
                   :on-blur handle-blur}]]
   [:div.input-group
    [:div.label
     [:label "Transaction Time"]]
    [:input.input {:type "date"
                   :name "transaction-date"
                   :value (get values "transaction-date")
                   :on-change handle-change
                   :on-blur handle-blur}]
    [:input.input {:type "time"
                   :name "transaction-time"
                   :value (get values "transaction-time")
                   :step "any"
                   :on-change handle-change
                   :on-blur handle-blur}]]])

(defn query-form
  []
  (let [{{:keys [path-params]} :data} @(rf/subscribe [::sub/current-route])
        {:keys [valid-date valid-time
                transaction-date transaction-time]}
        @(rf/subscribe [::sub/initial-date-time])]
    [fork/form {:path :query
                :form-id "query"
                :prevent-default? true
                :clean-on-unmount? true
                :initial-values {"eid" (:eid path-params)
                                 "valid-date" valid-date
                                 "valid-time" valid-time
                                 "transaction-date" transaction-date
                                 "transaction-time" transaction-time}
                :on-submit #(rf/dispatch [::events/go-to-query %])}
     (fn [{:keys [values
                  form-id
                  handle-change
                  handle-blur
                  submitting?
                  handle-submit] :as props}]
       [:<>
        [:form
         {:id form-id
          :on-submit handle-submit}
         [:textarea.textarea
          {:name "q"
           :value (get values "q")
           :on-change handle-change
           :on-blur handle-blur
           :rows 10}]
         [vt-tt-inputs props]
         [:button.button
          {:type "submit"}
          "Submit Query"]]])]))

(defn entity-form
  []
  (let [{:keys [path-params]} @(rf/subscribe [::sub/current-route])
        {:keys [valid-date valid-time
                transaction-date transaction-time]}
        @(rf/subscribe [::sub/initial-date-time])]
    [fork/form {:path :entity
                :form-id "entity"
                :prevent-default? true
                :clean-on-unmount? true
                :initial-values {"eid" (:eid path-params)
                                 "valid-date" valid-date
                                 "valid-time" valid-time
                                 "transaction-date" transaction-date
                                 "transaction-time" transaction-time}
                :on-submit #(rf/dispatch [::events/go-to-entity-view %])}
     (fn [{:keys [values
                  form-id
                  state
                  handle-change
                  handle-blur
                  submitting?
                  handle-submit] :as props}]
       [:<>
        [:form
         {:id form-id
          :on-submit handle-submit}
         [:textarea.textarea
          {:name "eid"
           :value (get values "eid")
           :on-change handle-change
           :on-blur handle-blur}]
         [vt-tt-inputs props]
         [:button.button
          {:type "submit"}
          "Submit Entity"]]])]))

(defn form
  []
  (let [search-view @(rf/subscribe [::sub/search-view])]
    (if (= :query search-view)
      [query-form]
      [entity-form])))

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])]
    [:<>
     (if error
       [:div.error-box error]
       [table/table data])]))

(defn query-view
  []
  (let [query-view @(rf/subscribe [::sub/query-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       {:class (if (= query-view :table)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-view :table])}
       "Table"]
      [:div.pane-nav__tab
       {:class (if (= query-view :graph)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-view :graph])}
       "Graph"]
      [:div.pane-nav__tab
       {:class (if (= query-view :range)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-view :range])}
       "Range"]]
     (case query-view
       :table [query-table]
       :graph [:div "this is graph"]
       :range [:div "this is range"])]))

(defn- entity->hiccup
  [links edn]
  (if-let [href (get links edn)]
    [:a.entity-link
     {:href href}
     (str edn)]
    (cond
      (map? edn) (for [[k v] edn]
                   ^{:key (str (gensym))}
                   [:div.entity-group
                    [:div.entity-group__key
                     (entity->hiccup links k)]
                    [:div.entity-group__value
                     (entity->hiccup links v)]])

      (sequential? edn) [:ol.entity-group__value
                         (for [v edn]
                           ^{:key (str (gensym))}
                           [:li (entity->hiccup links v)])]
      (set? edn) [:ul.entity-group__value
                  (for [v edn]
                    ^{:key v}
                    [:li (entity->hiccup links v)])]
      :else (str edn))))

(defn entity-document
  []
  (let [{:keys [linked-entities entity-result entity-name vt tt]}
        @(rf/subscribe [::sub/entity-view-data])
        loading? @(rf/subscribe [::sub/entity-loading?])]
    [:div.entity-map__container
     (if loading?
       [:div.entity-map.entity-map--loading
        [:i.fas.fa-spinner.entity-map__load-icon]]
       [:<>
        [:div.entity-map
         (if entity-result
           [:<>
            [:div.entity-group
             [:div.entity-group__key
              ":crux.db/id"]
             [:div.entity-group__value
              (str (:crux.db/id entity-result))]]
            [:hr.entity-group__separator]
            (entity->hiccup linked-entities
                            (dissoc entity-result :crux.db/id))]
           [:<> [:strong entity-name] " entity not found"])]
        [:div.entity-vt-tt
         [:div.entity-vt-tt__title
          "Valid Time"]
         [:div.entity-vt-tt__value vt]
         [:div.entity-vt-tt__title
          "Transaction Time"]
         [:div.entity-vt-tt__value tt]]])]))

(defn entity-view
  []
  (let [entity-view @(rf/subscribe [::sub/entity-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       {:class (if (= entity-view :document)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-entity-view :document])}
       "Document"]
      [:div.pane-nav__tab
       {:class (if (= entity-view :history)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-entity-view :history])}
       "History"]]
     (case entity-view
       :document [entity-document]
       :history [:div "this is history"])]))

(defn left-pane
  []
  (let [query-pane-show? @(rf/subscribe [::sub/query-pane-show?])
        search-view @(rf/subscribe [::sub/search-view])]
    [:div.left-pane
     (if query-pane-show?
       [:div.hide-button
        {:on-click #(rf/dispatch [::events/query-pane-toggle])}
        "Hide"]
       [:button.button.hidden-pane
        {:on-click #(rf/dispatch [::events/query-pane-toggle])}
        [:span "."]
        [:span "."]
        [:span "."]])
     [:div
      {:class (if query-pane-show?
                "pane-toggled"
                "pane-untoggled")}
      [:div.pane-nav
       [:div.pane-nav__tab
        {:class (if (= search-view :query)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-search-view :query])}
        "Query"]
       [:div.pane-nav__tab
        {:class (if (= search-view :entity)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-search-view :entity])}
        "Entity"]]
      [form]]]))

(defn view []
  (let [{{:keys [name]} :data} @(rf/subscribe [::sub/current-route])]
    [:<>
     [:pre (with-out-str (pprint/pprint @(rf/subscribe [::sub/current-route])))]
     [:div.container.page-pane
      [left-pane]
      [:div.right-pane
       [:div.back-button
        [:a
         {:on-click common/back-page}
         [:i.fas.fa-chevron-left]
         [:span.back-button__text "Back"]]]
       (case name
         :query [query-view]
         :entity [entity-view]
         [:div "no matching"])]]]))
