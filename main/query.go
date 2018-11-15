package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/TerrexTech/agg-itemdonate-report/report"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	tlog "github.com/TerrexTech/go-logtransport/log"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/pkg/errors"
)

// Query handles "query" events.
func Query(itemDonateColl *mongo.Collection, reportColl *mongo.Collection, event *model.Event) *model.KafkaResponse {

	brokersStr := os.Getenv("KAFKA_BROKERS")
	brokers := *commonutil.ParseHosts(brokersStr)
	logTopic := os.Getenv("KAFKA_LOG_PRODUCER_TOPIC")
	serviceName := os.Getenv("SERVICE_NAME")

	prodConfig := &kafka.ProducerConfig{
		KafkaBrokers: brokers,
	}
	logger, err := tlog.Init(nil, serviceName, prodConfig, logTopic)
	if err != nil {
		err = errors.Wrap(err, "Error initializing Logger")
		log.Fatalln(err)
	}

	//This is where it starts
	// event.Data should be in this format: `{"timestamp":{"$gt":1529315000},"timestamp":{"$lt":1551997372}}`

	filter := report.DonateItemParams{}

	var reportAgg []report.ReportResult

	err = json.Unmarshal(event.Data, &filter)
	if err != nil {
		err = errors.Wrap(err, "Query: Error while unmarshalling Event-data - ItemDonateReport")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, filter)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if &filter == nil {
		err = errors.New("blank filter provided")
		err = errors.Wrap(err, "Query left blank - ItemDonateReport")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, filter)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	avgDonateReport, err := report.ItemDonateReport(filter, itemDonateColl)
	if err != nil {
		err = errors.Wrap(err, "Error getting results from ItemDonateCollection")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, filter)
	}

	if len(avgDonateReport) < 1 {
		err = errors.New("Error: No result found from agg_itemdonate collection - Function = ItemDonateReport")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, reportAgg)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	for _, v := range avgDonateReport {
		m, assertOK := v.(map[string]interface{})
		if !assertOK {
			err = errors.New("Error getting results from asserting AvgSoldReport into map[string]interface{}")
			logger.E(tlog.Entry{
				Description: err.Error(),
				ErrorCode:   1,
			}, m)
		}

		groupByFields := m["_id"]
		mapInGroupBy := groupByFields.(map[string]interface{})
		sku := mapInGroupBy["sku"].(string)
		name := mapInGroupBy["name"].(string)

		// log.Println(m, "#############")

		//if it crashes on donateWeight - check the donateWeight field inside db and inside item_donate file--- inside the aggregate pipeline they should match
		reportAgg = append(reportAgg, report.ReportResult{
			SKU:          sku,
			Name:         name,
			DonateWeight: m["avg_donate"].(float64),
			TotalWeight:  m["avg_total"].(float64),
		})
	}

	reportID, err := uuuid.NewV4()
	if err != nil {
		err = errors.Wrap(err, "Error in generating reportID ")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		})
	}

	reportGen := report.DonateReport{
		ReportID:     reportID,
		SearchQuery:  filter,
		ReportResult: reportAgg,
	}

	repInsert, err := report.CreateReport(reportGen, reportColl)
	if err != nil {
		err = errors.Wrap(err, "Error in inserting report to mongo")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, reportGen)
	}

	log.Println(repInsert)
	log.Println(reportAgg, "$$$$$$$$$$$$$$$")

	resultMarshal, err := json.Marshal(reportAgg)
	if err != nil {
		err = errors.Wrap(err, "Query: Error marshalling report ItemDonateResults - called reportAgg")
		logger.E(tlog.Entry{
			Description: err.Error(),
			ErrorCode:   1,
		}, reportAgg)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		EventAction:   event.EventAction,
		Result:        resultMarshal,
		ServiceAction: event.ServiceAction,
		UUID:          event.UUID,
	}
}
