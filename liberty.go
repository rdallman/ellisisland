package main

import (
	"flag"
	"os"
	"sync"

	v2config "github.com/iron-io/iron_go/config"
	v2mq "github.com/iron-io/iron_go/mq"
	v3config "github.com/iron-io/iron_go3/config"
	v3mq "github.com/iron-io/iron_go3/mq"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	projectId2 = flag.String("v2_project_id", "", "what you doin")
	token2     = flag.String("v2_token", "", "who you is")
	from       = flag.String("from", "mq-aws-us-east-1.iron.io", "where they was")

	to         = flag.String("to", "mq-aws-us-east-1-1.iron.io", "where they goin to")
	projectId3 = flag.String("v3_project_id", "", "what you doin")
	token3     = flag.String("v3_token", "", "who you is")

	skipMessages = flag.Bool("skip-messages", false, "just the metas")
	queue        = flag.String("queue", "", "move just this queue")
	threads      = flag.Int("threads", 1, "number of queues to move at a time")

	help = flag.Bool("help", false, "show this")
	h    = flag.Bool("h", false, "show this")
)

func main() {
	flag.Parse()

	if *h || *help {
		flag.PrintDefaults()
		return
	}

	if *projectId2 == "" || *token2 == "" || *projectId3 == "" || *token3 == "" {
		log15.Error("give me your credentials bob")
		os.Exit(1)
	}
	v2settings := v2config.Config("iron_mq")
	v2settings.ProjectId = *projectId2
	v2settings.Token = *token2
	v2settings.Host = *from
	v2settings.Scheme = "https"
	v2settings.Port = 443

	v3settings := v3config.Config("iron_mq")
	v3settings.ProjectId = *projectId3
	v3settings.Token = *token3
	v3settings.Host = *to
	v3settings.Scheme = "https"
	v3settings.Port = 443

	if *queue != "" {
		moveQueue(v2mq.ConfigNew(*queue, &v2settings), &v3settings)
		return
	}

	var wg sync.WaitGroup
	wg.Add(*threads)
	qs := make(chan v2mq.Queue, *threads)
	for i := 0; i < *threads; i++ {
		go func() {
			defer wg.Done()
			for q := range qs {
				moveQueue(q, &v3settings)
			}
		}()
	}

	queues := getQueues(&v2settings)
	log15.Info("moving queues", "n", len(queues))
	for _, q := range queues {
		qs <- q
	}
	close(qs)

	wg.Wait()
	log15.Info("doneso")
}

func getQueues(settings *v2config.Settings) []v2mq.Queue {
	var queues []v2mq.Queue
	for page := 0; ; page++ {
		qs, err := v2mq.ListSettingsQueues(*settings, page, 100)
		if err != nil {
			log15.Error("error getting queues", "page", page, "project_id", settings.ProjectId, "err", err)
		}
		queues = append(queues, qs...)
		if len(qs) < 100 {
			break
		}
	}
	return queues
}

func moveQueue(q2 v2mq.Queue, settings *v3config.Settings) {
	info, err := q2.Info()
	if err != nil {
		log15.Error("error getting queue info", "err", err, "q", q2.Name)
		return
	}
	v3info := v3mq.QueueInfo{Name: info.Name}
	if info.PushType != "" {
		v3info.Type = &info.PushType
		push := new(v3mq.PushInfo)
		push.Retries = info.Retries
		push.RetriesDelay = info.RetriesDelay
		push.ErrorQueue = info.ErrorQueue
		for _, s := range info.Subscribers {
			push.Subscribers = append(push.Subscribers, v3mq.QueueSubscriber{URL: s.URL, Headers: s.Headers})
		}
		v3info.Push = push
	} else {
		pull := "pull"
		v3info.Type = &pull
	}
	if _, err := v3mq.ConfigCreateQueue(v3info, settings); err != nil {
		log15.Error("error making v3 queue", "err", err, "q", q2.Name)
		return
	}
	if info.PushType != "" {
		log15.Info("push queue, skipping message migration")
		return
	} else if *skipMessages {
		log15.Info("skipping messages because you told me to")
		return
	}

	log15.Info("moving messages", "q", q2.Name, "n", info.Size)

	q3 := v3mq.ConfigNew(q2.Name, settings)
	for {
		msgs, err := q2.GetN(100)
		if err != nil {
			log15.Error("error getting messages from v2 queue", "err", err, "q", q2.Name)
			return
		} else if len(msgs) == 0 {
			return
		}

		bodies := make([]string, 0, len(msgs))
		for _, msg := range msgs {
			bodies = append(bodies, msg.Body)
		}
		_, err = q3.PushStrings(bodies...)
		if err != nil {
			log15.Error("error pushing messages to v3", "err", err, "q", q2.Name)
			return
		}
		for _, msg := range msgs {
			msg.Delete()
		}
	}
}
