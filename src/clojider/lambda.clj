(ns clojider.lambda
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.java.io :as io]
            [clj-time.core :refer [millis]]
            [clj-gatling.pipeline :as pipeline]
            [clj-gatling.schema :as schema]
            [cheshire.core :refer [generate-stream parse-stream]]
            [schema.core :as s])
  (:import (java.io File)))

(defn- run-simulation [input]
  (let [collector-as-symbol (fn [reporter]
                              (-> reporter
                                (update :collector read-string)
                                (update :reporter-key keyword)))
        options (-> (:options input)
                  (update :duration millis)
                  (assoc :results-dir (System/getProperty "java.io.tmpdir"))
                  (assoc :error-file (-> (doto (File. (System/getProperty "java.io.tmpdir") "error.log")
                                           (.createNewFile))
                                       (.getAbsolutePath)))
                  (update :reporters #(map collector-as-symbol %)))]
    (pipeline/simulation-runner (read-string (:simulation input)) options)))

(deflambdafn clojider.LambdaFn
  [is os ctx]
  (let [input (parse-stream (io/reader is) true)
        output (io/writer os)]
    (println "Running simulation with config" input)
    (let [result (run-simulation input)]
      (println "Returning result" result)
      (generate-stream result output)
      (.flush output))))
