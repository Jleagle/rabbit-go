package rabbit

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
)

const (
	RangeOneMinute  Range = "1m"
	RangeTenMinutes Range = "10m"
	RangeOneHour    Range = "1h"
	RangeEightHours Range = "8h"
	RangeOneDay     Range = "1d"
)

type Range string

type Payload struct {
	LengthsAge    int `json:"lengths_age"`
	LengthsIncr   int `json:"lengths_incr"`
	MsgRatesAge   int `json:"msg_rates_age"`
	MsgRatesIncr  int `json:"msg_rates_incr"`
	DataRatesAge  int `json:"data_rates_age"`
	DataRatesIncr int `json:"data_rates_incr"`
}

func (p *Payload) Preset(rangex Range) {

	switch rangex {
	case RangeOneMinute:
		p.LengthsAge = 60
		p.LengthsIncr = 5
		p.MsgRatesAge = 60
		p.MsgRatesIncr = 5
		p.DataRatesAge = 60
		p.DataRatesIncr = 5
	case RangeTenMinutes:
		p.LengthsAge = 600
		p.LengthsIncr = 5
		p.MsgRatesAge = 600
		p.MsgRatesIncr = 5
		p.DataRatesAge = 600
		p.DataRatesIncr = 5
	case RangeOneHour:
		p.LengthsAge = 3600
		p.LengthsIncr = 60
		p.MsgRatesAge = 3600
		p.MsgRatesIncr = 60
		p.DataRatesAge = 3600
		p.DataRatesIncr = 60
	case RangeEightHours:
		p.LengthsAge = 28800
		p.LengthsIncr = 600
		p.MsgRatesAge = 28800
		p.MsgRatesIncr = 600
		p.DataRatesAge = 28800
		p.DataRatesIncr = 600
	case RangeOneDay:
		p.LengthsAge = 86400
		p.LengthsIncr = 1800
		p.MsgRatesAge = 86400
		p.MsgRatesIncr = 1800
		p.DataRatesAge = 86400
		p.DataRatesIncr = 1800
	}
}

type Rabbit struct {
	Host     string
	Port     string
	Username string
	Password string
}

func (r Rabbit) GetOverview(payload Payload) (resp Overview, err error) {

	r.checkConnection()

	values := url.Values{}
	values.Set("lengths_age", strconv.Itoa(payload.LengthsAge))
	values.Set("lengths_incr", strconv.Itoa(payload.LengthsIncr))
	values.Set("msg_rates_age", strconv.Itoa(payload.MsgRatesAge))
	values.Set("msg_rates_incr", strconv.Itoa(payload.MsgRatesIncr))

	req, err := http.NewRequest("GET", "http://"+r.Host+":"+r.Port+"/api/overview?"+values.Encode(), nil)
	if err != nil {
		return resp, err
	}

	req.SetBasicAuth(r.Username, r.Password)

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return resp, err
	}

	// Close read
	defer func(response *http.Response) {
		err := response.Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(response)

	// Convert to bytes
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return resp, err
	}

	// Fix JSON
	regex := regexp.MustCompile(`"socket_opts":\[]`)
	s := regex.ReplaceAllString(string(bytes), `"socket_opts":{}`)

	bytes = []byte(s)

	// Unmarshal JSON
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (r Rabbit) GetQueue(queue QueueName, payload Payload) (resp Queue, err error) {

	r.checkConnection()

	values := url.Values{}
	values.Set("lengths_age", strconv.Itoa(payload.LengthsAge))
	values.Set("lengths_incr", strconv.Itoa(payload.LengthsIncr))
	values.Set("msg_rates_age", strconv.Itoa(payload.MsgRatesAge))
	values.Set("msg_rates_incr", strconv.Itoa(payload.MsgRatesIncr))
	values.Set("data_rates_age", strconv.Itoa(payload.DataRatesAge))
	values.Set("data_rates_incr", strconv.Itoa(payload.DataRatesIncr))

	req, err := http.NewRequest("GET", "http://"+r.Host+":"+r.Port+"/api/queues/%2F/"+string(queue)+"?"+values.Encode(), nil)
	if err != nil {
		return resp, err
	}

	req.SetBasicAuth(r.Username, r.Password)

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return resp, err
	}

	// Close read
	defer func(response *http.Response) {
		err := response.Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(response)

	// Convert to bytes
	bytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return resp, err
	}

	// Unmarshal JSON
	err = json.Unmarshal(bytes, &resp)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (r Rabbit) checkConnection() {

	if r.Username == "" {
		r.Username = "guest"
	}
	if r.Password == "" {
		r.Password = "guest"
	}
	if r.Host == "" {
		r.Host = "localhost"
	}
	if r.Port == "" {
		r.Port = "15672"
	}
}

type Overview struct {
	ManagementVersion string `json:"management_version"`
	RatesMode         string `json:"rates_mode"`
	ExchangeTypes     []struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Enabled     bool   `json:"enabled"`
	} `json:"exchange_types"`
	RabbitmqVersion   string `json:"rabbitmq_version"`
	ClusterName       string `json:"cluster_name"`
	ErlangVersion     string `json:"erlang_version"`
	ErlangFullVersion string `json:"erlang_full_version"`
	MessageStats      struct {
		Ack                     int     `json:"ack"`
		AckDetails              Details `json:"ack_details"`
		Confirm                 int     `json:"confirm"`
		ConfirmDetails          Details `json:"confirm_details"`
		Deliver                 int     `json:"deliver"`
		DeliverDetails          Details `json:"deliver_details"`
		DeliverGet              int     `json:"deliver_get"`
		DeliverGetDetails       Details `json:"deliver_get_details"`
		DeliverNoAck            int     `json:"deliver_no_ack"`
		DeliverNoAckDetails     Details `json:"deliver_no_ack_details"`
		DiskReads               int     `json:"disk_reads"`
		DiskReadsDetails        Details `json:"disk_reads_details"`
		DiskWrites              int     `json:"disk_writes"`
		DiskWritesDetails       Details `json:"disk_writes_details"`
		Get                     int     `json:"get"`
		GetDetails              Details `json:"get_details"`
		GetNoAck                int     `json:"get_no_ack"`
		GetNoAckDetails         Details `json:"get_no_ack_details"`
		Publish                 int     `json:"publish"`
		PublishDetails          Details `json:"publish_details"`
		Redeliver               int     `json:"redeliver"`
		RedeliverDetails        Details `json:"redeliver_details"`
		ReturnUnroutable        int     `json:"return_unroutable"`
		ReturnUnroutableDetails Details `json:"return_unroutable_details"`
	} `json:"message_stats"`
	QueueTotals struct {
		Messages                      int     `json:"messages"`
		MessagesDetails               Details `json:"messages_details"`
		MessagesReady                 int     `json:"messages_ready"`
		MessagesReadyDetails          Details `json:"messages_ready_details"`
		MessagesUnacknowledged        int     `json:"messages_unacknowledged"`
		MessagesUnacknowledgedDetails Details `json:"messages_unacknowledged_details"`
	} `json:"queue_totals"`
	ObjectTotals struct {
		Channels    int `json:"channels"`
		Connections int `json:"connections"`
		Consumers   int `json:"consumers"`
		Exchanges   int `json:"exchanges"`
		Queues      int `json:"queues"`
	} `json:"object_totals"`
	StatisticsDbEventQueue int    `json:"statistics_db_event_queue"`
	Node                   string `json:"node"`
	Listeners              []struct {
		Node       string `json:"node"`
		Protocol   string `json:"protocol"`
		IPAddress  string `json:"ip_address"`
		Port       int    `json:"port"`
		SocketOpts struct {
			Backlog     int           `json:"backlog"`
			Nodelay     bool          `json:"nodelay"`
			Linger      []interface{} `json:"linger"`
			ExitOnClose bool          `json:"exit_on_close"`
		} `json:"socket_opts"`
	} `json:"listeners"`
	Contexts []struct {
		SslOpts     []interface{} `json:"ssl_opts"`
		Node        string        `json:"node"`
		Description string        `json:"description"`
		Path        string        `json:"path"`
		Port        string        `json:"port"`
		Ssl         string        `json:"ssl"`
	} `json:"contexts"`
}

type Queue struct {
	ConsumerDetails []interface{} `json:"consumer_details"`
	Arguments       struct {
	} `json:"arguments"`
	AutoDelete         bool `json:"auto_delete"`
	BackingQueueStatus struct {
		AvgAckEgressRate  float64       `json:"avg_ack_egress_rate"`
		AvgAckIngressRate float64       `json:"avg_ack_ingress_rate"`
		AvgEgressRate     float64       `json:"avg_egress_rate"`
		AvgIngressRate    float64       `json:"avg_ingress_rate"`
		Delta             []interface{} `json:"delta"`
		Len               int           `json:"len"`
		Mode              string        `json:"mode"`
		NextSeqID         int           `json:"next_seq_id"`
		Q1                int           `json:"q1"`
		Q2                int           `json:"q2"`
		Q3                int           `json:"q3"`
		Q4                int           `json:"q4"`
		TargetRAMCount    string        `json:"target_ram_count"`
	} `json:"backing_queue_status"`
	ConsumerUtilisation       interface{}   `json:"consumer_utilisation"`
	Consumers                 int           `json:"consumers"`
	Deliveries                []interface{} `json:"deliveries"`
	Durable                   bool          `json:"durable"`
	EffectivePolicyDefinition []interface{} `json:"effective_policy_definition"`
	Exclusive                 bool          `json:"exclusive"`
	ExclusiveConsumerTag      interface{}   `json:"exclusive_consumer_tag"`
	GarbageCollection         struct {
		FullsweepAfter  int `json:"fullsweep_after"`
		MaxHeapSize     int `json:"max_heap_size"`
		MinBinVheapSize int `json:"min_bin_vheap_size"`
		MinHeapSize     int `json:"min_heap_size"`
		MinorGcs        int `json:"minor_gcs"`
	} `json:"garbage_collection"`
	HeadMessageTimestamp       interface{}   `json:"head_message_timestamp"`
	IdleSince                  string        `json:"idle_since"`
	Incoming                   []interface{} `json:"incoming"`
	Memory                     int           `json:"memory"`
	MessageBytes               int           `json:"message_bytes"`
	MessageBytesPagedOut       int           `json:"message_bytes_paged_out"`
	MessageBytesPersistent     int           `json:"message_bytes_persistent"`
	MessageBytesRAM            int           `json:"message_bytes_ram"`
	MessageBytesReady          int           `json:"message_bytes_ready"`
	MessageBytesUnacknowledged int           `json:"message_bytes_unacknowledged"`
	MessageStats               struct {
		Ack                 int     `json:"ack"`
		AckDetails          Details `json:"ack_details"`
		Deliver             int     `json:"deliver"`
		DeliverDetails      Details `json:"deliver_details"`
		DeliverGet          int     `json:"deliver_get"`
		DeliverGetDetails   Details `json:"deliver_get_details"`
		DeliverNoAck        int     `json:"deliver_no_ack"`
		DeliverNoAckDetails Details `json:"deliver_no_ack_details"`
		Get                 int     `json:"get"`
		GetDetails          Details `json:"get_details"`
		GetNoAck            int     `json:"get_no_ack"`
		GetNoAckDetails     Details `json:"get_no_ack_details"`
		Publish             int     `json:"publish"`
		PublishDetails      Details `json:"publish_details"`
		Redeliver           int     `json:"redeliver"`
		RedeliverDetails    Details `json:"redeliver_details"`
	} `json:"message_stats"`
	Messages        int `json:"messages"`
	MessagesDetails struct {
		Avg     float64  `json:"avg"`
		AvgRate float64  `json:"avg_rate"`
		Rate    float64  `json:"rate"`
		Samples []Sample `json:"samples"`
	} `json:"messages_details"`
	MessagesPagedOut     int `json:"messages_paged_out"`
	MessagesPersistent   int `json:"messages_persistent"`
	MessagesRAM          int `json:"messages_ram"`
	MessagesReady        int `json:"messages_ready"`
	MessagesReadyDetails struct {
		Avg     float64  `json:"avg"`
		AvgRate float64  `json:"avg_rate"`
		Rate    float64  `json:"rate"`
		Samples []Sample `json:"samples"`
	} `json:"messages_ready_details"`
	MessagesReadyRAM              int `json:"messages_ready_ram"`
	MessagesUnacknowledged        int `json:"messages_unacknowledged"`
	MessagesUnacknowledgedDetails struct {
		Avg     float64  `json:"avg"`
		AvgRate float64  `json:"avg_rate"`
		Rate    float64  `json:"rate"`
		Samples []Sample `json:"samples"`
	} `json:"messages_unacknowledged_details"`
	MessagesUnacknowledgedRAM int         `json:"messages_unacknowledged_ram"`
	Name                      string      `json:"name"`
	Node                      string      `json:"node"`
	OperatorPolicy            interface{} `json:"operator_policy"`
	Policy                    interface{} `json:"policy"`
	RecoverableSlaves         interface{} `json:"recoverable_slaves"`
	Reductions                int         `json:"reductions"`
	ReductionsDetails         struct {
		Avg     float64  `json:"avg"`
		AvgRate float64  `json:"avg_rate"`
		Rate    float64  `json:"rate"`
		Samples []Sample `json:"samples"`
	} `json:"reductions_details"`
	State string `json:"state"`
	Vhost string `json:"vhost"`
}

type Details struct {
	Rate    float64  `json:"rate"`
	Samples []Sample `json:"samples"`
	AvgRate float64  `json:"avg_rate"`
	Avg     float64  `json:"avg"`
}

type Sample struct {
	Sample    int   `json:"sample"`
	Timestamp int64 `json:"timestamp"`
}
