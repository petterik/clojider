(ns clojider.rc
 (:require [clojider.aws :refer [aws-credentials]]
           [clojure.java.io :as io]
           [clojure.core.async :as async :refer [go go-loop >! <! >!! <!!]]
           [clj-time.core :as t]
           [clj-gatling.core :as gatling]
           [cheshire.core :refer [generate-string parse-stream]])
  (:import [com.amazonaws ClientConfiguration]
           [com.amazonaws.regions Regions]
           [com.amazonaws.services.lambda.model InvokeRequest InvocationType]
           [com.amazonaws.services.lambda AWSLambdaClient]))

(defn parse-result [result]
  (-> result
      (.getPayload)
      (.array)
      (java.io.ByteArrayInputStream.)
      (io/reader)
      (parse-stream true)))

(def lambda-client
  (memoize
    (fn [region]
      (let [client-config (-> (ClientConfiguration.)
                            (.withSocketTimeout (* 6 60 1000))
                            (.withMaxConnections 2))]
        (-> (AWSLambdaClient. @aws-credentials client-config)
          (.withRegion (Regions/fromName region)))))))

(defn invoke-lambda [simulation lambda-function-name options]
  (println "Invoking Lambda for node:" (:node-id options))
  (let [throttle? (pos? (:throttle options))
        client (lambda-client (-> options :context :region))
        request (-> (InvokeRequest.)
                  (.withFunctionName lambda-function-name)
                  (cond-> throttle? (.withInvocationType InvocationType/Event))
                  (.withPayload (generate-string {:simulation simulation
                                                  :options    options})))]
    (if throttle?
      (do
        (.invoke client request)
        nil)
      (parse-result (.invoke client request)))))

(def max-runtime-in-millis (* 4 60 1000))

(defn split-to-durations [millis]
  (loop [millis-left millis
         buckets []]
    (if (> millis-left max-runtime-in-millis)
      (recur (- millis-left max-runtime-in-millis) (conj buckets max-runtime-in-millis))
      (conj buckets millis-left))))

(defn invoke-lambda-sequentially [simulation lambda-function-name options node-id]
  (let [durations (split-to-durations (t/in-millis (:duration options)))]
    (apply merge-with concat
           (mapv #(invoke-lambda simulation
                                 lambda-function-name
                                 (assoc options
                                        :node-id node-id
                                        :duration %
                                        :timeout-in-ms (:timeout-in-ms options)))
                 durations))))

(defn lambda-executor [lambda-function-name node-id simulation options]
  (println "Starting AWS Lambda executor with id:" node-id)
  (let [only-collector-as-str (fn [reporter]
                                (-> reporter
                                    (dissoc :generator)
                                    (update :collector str)))]
    (invoke-lambda-sequentially (str simulation)
                                lambda-function-name
                                (update options :reporters #(map only-collector-as-str %))
                                node-id)))

(defn run-simulation [^clojure.lang.Symbol simulation
                      {:keys [concurrency
                              node-count
                              bucket-name
                              reporters
                              timeout-in-ms
                              duration
                              region]
                       :or {node-count 1}}]
  (gatling/run simulation (-> {:context       {:region region :bucket-name bucket-name}
                               :concurrency   concurrency
                               :timeout-in-ms timeout-in-ms
                               :reporters     reporters
                               :duration      duration
                               :nodes         node-count
                               :executor      (partial lambda-executor "clojider-load-testing-lambda")})))

(defn no-op-reporter []
  {:reporter-key :no-op-reporter
   :collector    (fn [{:keys [context results-dir]}]
                   {:collect (constantly nil)
                    :combine (constantly nil)})
   :generator    (fn [{:keys [context results-dir]}]
                   {:generate (constantly nil)
                    :as-str   (constantly nil)})})

(defn run-throttled-simulation [^clojure.lang.Symbol simulation
                            {:keys [concurrency
                                    node-count
                                    bucket-name
                                    timeout-in-ms
                                    duration
                                    region
                                    throttle]
                             :or {node-count 1}}]
  (let [in (async/chan)
        lambdas-per-second throttle
        agents (vec (repeatedly lambdas-per-second #(agent nil)))]
    (go-loop []
      (let [ttl (async/timeout 1000)]
        (loop [remaining lambdas-per-second]
          (when (pos? remaining)
            (when-let [f (<! in)]
              (send-off (get agents (dec remaining)) (fn [& _] (f)))
              (recur (dec remaining)))))
        (<! ttl))
      (recur))
    (gatling/run simulation (-> {:context       {:region region :bucket-name bucket-name}
                                 :concurrency   concurrency
                                 :timeout-in-ms timeout-in-ms
                                 :reporters     [(no-op-reporter)]
                                 :duration      duration
                                 :nodes         node-count
                                 :executor      (fn [node-id simulation options]
                                                  (let [ch (async/chan)
                                                        executor-fn
                                                        (fn []
                                                          (lambda-executor "clojider-load-testing-lambda"
                                                                           node-id
                                                                           simulation
                                                                           (assoc options :throttle throttle))
                                                          (async/close! ch))]
                                                    (>!! in executor-fn)
                                                    (<!! ch)))}))))
