// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"context"
	"fmt"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	influxdbClient "github.com/influxdata/influxdb/client/v2"
	"github.com/json-iterator/go"
	statsdClient "gopkg.in/alexcesaro/statsd.v2"
	"net"
	"net/http"
	"strings"
	"time"
)

func (p *Proxy) startMetricsReporter(d time.Duration, do, cleanup func() error) {
	go func() {
		if cleanup != nil {
			defer cleanup()
		}
		var ticker = time.NewTicker(d)
		defer ticker.Stop()
		var delay = &DelayExp2{
			Min: 1, Max: 15,
			Unit: time.Second,
		}
		for !p.IsClosed() {
			<-ticker.C
			if err := do(); err != nil {
				log.WarnErrorf(err, "report metrics failed")
				delay.SleepWithCancel(p.IsClosed)
			} else {
				delay.Reset()
			}
		}
	}()
}

func (p *Proxy) startMetricsJson() {
	server := p.config.MetricsReportServer
	period := p.config.MetricsReportPeriod.Duration()
	if server == "" {
		return
	}
	period = math2.MaxDuration(time.Second, period)

	p.startMetricsReporter(period, func() error {
		return rpc.ApiPostJson(server, p.Overview(StatsRuntime))
	}, nil)
}

func (p *Proxy) startMetricsInfluxdb() {
	server := p.config.MetricsReportInfluxdbServer
	period := p.config.MetricsReportInfluxdbPeriod.Duration()
	if server == "" {
		return
	}
	period = math2.MaxDuration(time.Second, period)

	c, err := influxdbClient.NewHTTPClient(influxdbClient.HTTPConfig{
		Addr:     server,
		Username: p.config.MetricsReportInfluxdbUsername,
		Password: p.config.MetricsReportInfluxdbPassword,
		Timeout:  time.Second * 5,
	})
	if err != nil {
		log.WarnErrorf(err, "create influxdb client failed")
		return
	}

	database := p.config.MetricsReportInfluxdbDatabase

	p.startMetricsReporter(period, func() error {
		b, err := influxdbClient.NewBatchPoints(influxdbClient.BatchPointsConfig{
			Database:  database,
			Precision: "ns",
		})
		if err != nil {
			return errors.Trace(err)
		}
		model := p.Model()
		stats := p.Stats(StatsRuntime)

		tags := map[string]string{
			"data_center":  model.DataCenter,
			"token":        model.Token,
			"product_name": model.ProductName,
			"admin_addr":   model.AdminAddr,
			"proxy_addr":   model.ProxyAddr,
			"hostname":     model.Hostname,
		}
		fields := map[string]interface{}{
			"ops_total":                stats.Ops.Total,
			"ops_fails":                stats.Ops.Fails,
			"ops_redis_errors":         stats.Ops.Redis.Errors,
			"ops_qps":                  stats.Ops.QPS,
			"sessions_total":           stats.Sessions.Total,
			"sessions_alive":           stats.Sessions.Alive,
			"rusage_mem":               stats.Rusage.Mem,
			"rusage_cpu":               stats.Rusage.CPU,
			"runtime_gc_num":           stats.Runtime.GC.Num,
			"runtime_gc_total_pausems": stats.Runtime.GC.TotalPauseMs,
			"runtime_num_procs":        stats.Runtime.NumProcs,
			"runtime_num_goroutines":   stats.Runtime.NumGoroutines,
			"runtime_num_cgo_call":     stats.Runtime.NumCgoCall,
			"runtime_num_mem_offheap":  stats.Runtime.MemOffheap,
		}
		p, err := influxdbClient.NewPoint("codis_usage", tags, fields, time.Now())
		if err != nil {
			return errors.Trace(err)
		}
		b.AddPoint(p)
		return c.Write(b)
	}, func() error {
		return c.Close()
	})
}

func (p *Proxy) startMetricsStatsd() {
	server := p.config.MetricsReportStatsdServer
	period := p.config.MetricsReportStatsdPeriod.Duration()
	if server == "" {
		return
	}
	period = math2.MaxDuration(time.Second, period)

	c, err := statsdClient.New(statsdClient.Address(server))
	if err != nil {
		log.WarnErrorf(err, "create statsd client failed")
		return
	}

	var (
		prefix   = p.config.MetricsReportStatsdPrefix
		replacer = strings.NewReplacer(".", "_", ":", "_")
	)

	p.startMetricsReporter(period, func() error {
		model := p.Model()
		stats := p.Stats(StatsRuntime)

		segs := []string{
			prefix, model.ProductName,
			replacer.Replace(model.AdminAddr),
			replacer.Replace(model.ProxyAddr),
		}

		fields := map[string]interface{}{
			"ops_total":                stats.Ops.Total,
			"ops_fails":                stats.Ops.Fails,
			"ops_redis_errors":         stats.Ops.Redis.Errors,
			"ops_qps":                  stats.Ops.QPS,
			"sessions_total":           stats.Sessions.Total,
			"sessions_alive":           stats.Sessions.Alive,
			"rusage_mem":               stats.Rusage.Mem,
			"rusage_cpu":               stats.Rusage.CPU,
			"runtime_gc_num":           stats.Runtime.GC.Num,
			"runtime_gc_total_pausems": stats.Runtime.GC.TotalPauseMs,
			"runtime_num_procs":        stats.Runtime.NumProcs,
			"runtime_num_goroutines":   stats.Runtime.NumGoroutines,
			"runtime_num_cgo_call":     stats.Runtime.NumCgoCall,
			"runtime_num_mem_offheap":  stats.Runtime.MemOffheap,
		}
		for key, value := range fields {
			c.Gauge(strings.Join(append(segs, key), "."), value)
		}
		return nil
	}, func() error {
		c.Close()
		return nil
	})
}

func (p *Proxy) startMetricsOpenTSDB() {
	server := p.config.MetricsReportOpenTSDBServer
	period := p.config.MetricsReportOpenTSDBPeriod.Duration()
	if server == "" {
		return
	}
	period = math2.MaxDuration(time.Second, period)

	c := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial(network, addr)
			},
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: 10 * time.Second,
	}
	type Metric struct {
		Metric    interface{} `json:"metric"`
		Timestamp interface{} `json:"timestamp"`
		Value     interface{} `json:"value"`
		Tags      interface{} `json:"tags"`
	}

	type Tag struct {
		DataCenter  interface{} `json:"data_center,omitempty"`
		Token       interface{} `json:"token,omitempty"`
		ProductName interface{} `json:"product_name,omitempty"`
		AdminAddr   interface{} `json:"admin_addr,omitempty"`
		ProxyAddr   interface{} `json:"proxy_addr,omitempty"`
		Hostname    interface{} `json:"hostname,omitempty"`
	}
	tags := &Tag{
		DataCenter:  p.model.DataCenter,
		Token:       p.model.Token,
		ProductName: p.model.ProductName,
		AdminAddr:   p.model.AdminAddr,
		ProxyAddr:   p.model.ProxyAddr,
		Hostname:    p.model.Hostname,
	}
	openTsUrl := fmt.Sprintf("http://%s/api/put?details", server)

	HttpToOpenTSDB := func(httpClient *http.Client, method, url, body interface{}) (*http.Response, error) {
		b, err := jsoniter.Marshal(body)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(method.(string), url.(string), strings.NewReader(string(b)))
		if err != nil {
			return nil, err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	p.startMetricsReporter(period, func() error {
		stats := p.Stats(StatsRuntime)

		fields := map[string]interface{}{
			"ops_total":                stats.Ops.Total,
			"ops_fails":                stats.Ops.Fails,
			"ops_redis_errors":         stats.Ops.Redis.Errors,
			"ops_qps":                  stats.Ops.QPS,
			"sessions_total":           stats.Sessions.Total,
			"sessions_alive":           stats.Sessions.Alive,
			"rusage_mem":               stats.Rusage.Mem,
			"rusage_cpu":               stats.Rusage.CPU,
			"runtime_gc_num":           stats.Runtime.GC.Num,
			"runtime_gc_total_pausems": stats.Runtime.GC.TotalPauseMs,
			"runtime_num_procs":        stats.Runtime.NumProcs,
			"runtime_num_goroutines":   stats.Runtime.NumGoroutines,
			"runtime_num_cgo_call":     stats.Runtime.NumCgoCall,
			"runtime_num_mem_offheap":  stats.Runtime.MemOffheap,
		}
		timestamp := time.Now().Unix()
		var metrics []Metric
		for key, value := range fields {
			metrics = append(metrics, Metric{
				Metric:    key,
				Value:     value,
				Timestamp: timestamp,
				Tags:      tags,
			})
		}
		res, err := HttpToOpenTSDB(c, "POST", openTsUrl, metrics)
		if err != nil {
			log.Infof("send metrics to opentsdb error %v, res=%v", err, fmt.Sprint(jsoniter.MarshalToString(res)))
		}
		return nil
	}, func() error {
		return nil
	})
}
