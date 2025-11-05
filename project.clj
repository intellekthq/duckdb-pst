(defproject duckdb-pst "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :plugins [[org.jank-lang/lein-jank "0.2"]]
  :dependencies [[org.clojure/clojure "1.11.1"]]
  :main ^:skip-aot com.intellekt.duckpst
  :jank {:include-dirs ["/opt/homebrew/lib/jank/0.1/include/c++/v1"
                        "microsoft-pst-sdk/sourceCode/fairport/trunk"
                        "/opt/homebrew/include"
                        "src/cxx/include"]
         :library-dirs ["/opt/homebrew/lib"]}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
