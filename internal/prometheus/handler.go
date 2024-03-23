package prometheus

import "github.com/prometheus/client_golang/prometheus/promhttp"

var Handler = promhttp.Handler()