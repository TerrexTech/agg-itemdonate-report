package report

import (
	ctx "context"
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-mongoutils/mongo"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBooks(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Books Suite")
}

// newTimeoutContext creates a new WithTimeout context with specified timeout.
func newTimeoutContext(timeout uint32) (ctx.Context, ctx.CancelFunc) {
	return ctx.WithTimeout(
		ctx.Background(),
		time.Duration(timeout)*time.Millisecond,
	)
}

var _ = Describe("Mongo service test", func() {
	var (
		// jsonString string
		mgTable *mongo.Collection
		client  *mongo.Client

		item1 DonateItem
		item2 DonateItem
	)

	dropTestDatabase := func() {
		var err error
		client, err = mongo.NewClient(mongo.ClientConfig{
			Hosts:               []string{"mongo:27017"},
			Username:            "root",
			Password:            "root",
			TimeoutMilliseconds: 5000,
		})
		Expect(err).ToNot(HaveOccurred())

		dbCtx, dbCancel := newTimeoutContext(5000)
		err = client.Database("rns_test").Drop(dbCtx)
		dbCancel()
		Expect(err).ToNot(HaveOccurred())

		err = client.Disconnect()
		Expect(err).ToNot(HaveOccurred())
	}

	createTestDatabase := func(collectionName string, schema interface{}) {
		var err error
		client, err = mongo.NewClient(mongo.ClientConfig{
			Hosts:               []string{"mongo:27017"},
			Username:            "root",
			Password:            "root",
			TimeoutMilliseconds: 5000,
		})
		Expect(err).ToNot(HaveOccurred())

		conn := &mongo.ConnectionConfig{
			Client:  client,
			Timeout: 1000,
		}

		mgTable, err = mongo.EnsureCollection(&mongo.Collection{
			Connection:   conn,
			Name:         collectionName,
			Database:     "rns_test",
			SchemaStruct: schema,
		})
		Expect(err).ToNot(HaveOccurred())
	}

	BeforeEach(func() {

		createTestDatabase("mtest", &DonateItem{})
		itemID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		donateID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		item1 = DonateItem{
			ItemID:      itemID,
			DonateID:    donateID,
			SKU:         "test-sku1",
			Name:        "test-name1",
			Lot:         "test-lot1",
			Weight:      101,
			TotalWeight: 120,
			Timestamp:   10,
		}
		_, err = mgTable.InsertOne(item1)
		Expect(err).ToNot(HaveOccurred())

		itemID, err = uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		donateID, err = uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		item2 = DonateItem{
			ItemID:      itemID,
			DonateID:    donateID,
			SKU:         "test-sku2",
			Name:        "test-name2",
			Lot:         "test-lot1",
			Weight:      105,
			TotalWeight: 140,
			Timestamp:   20,
		}
		_, err = mgTable.InsertOne(item2)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		dropTestDatabase()
		err := client.Disconnect()
		Expect(err).ToNot(HaveOccurred())
	})

	It("Confirm inserted ItemID field", func() {
		var findResults []interface{}
		findResults, err := mgTable.Find(map[string]interface{}{
			"itemID": map[string]interface{}{
				"$eq": item1.ItemID,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		for _, v := range findResults {
			m, assertOK := v.(map[string]interface{})
			Expect(assertOK).To(BeTrue())
			log.Println(m)
			Expect(m["itemID"]).To(Equal(item1.ItemID.String()))
			Expect(m["itemID"]).To(Equal(item2.ItemID.String()))
		}
	})

	It("Confirm inserted DonateID field", func() {
		var findResults []interface{}
		findResults, err := mgTable.Find(map[string]interface{}{
			"donateID": map[string]interface{}{
				"$eq": item1.DonateID,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		for _, v := range findResults {
			m, assertOK := v.(map[string]interface{})
			Expect(assertOK).To(BeTrue())
			log.Println(m["avg_donate"])
			Expect(m["donateID"]).To(Equal(item1.DonateID.String()))
		}
	})

	It("Check avgSold for inserted records", func() {
		searchParameters := []byte(`{"timestamp":{"$gt":9},"timestamp":{"$lt":21}}`)

		x := DonateItemParams{}
		err := json.Unmarshal(searchParameters, &x)
		Expect(err).ToNot(HaveOccurred())

		avgDonateReport, err := ItemDonateReport(x, mgTable)
		Expect(err).ToNot(HaveOccurred())

		log.Println(avgDonateReport, "*******************")

		for _, v := range avgDonateReport {
			m, assertOK := v.(map[string]interface{})
			Expect(assertOK).To(BeTrue())
			log.Println(m["_id"])
			log.Println(m)

			avgSold, assertOK := m["avg_donate"].(float64)
			Expect(assertOK).To(BeTrue())
			log.Println(avgSold)
			if avgSold == item1.Weight {
				Expect(avgSold).To(Equal(item1.Weight))
				log.Println(item1.Weight)
			} else {
				Expect(avgSold).To(Equal(item2.Weight))
				log.Println(item2.Weight)
			}

		}
	})

	It("Error when timestamp is empty", func() {
		searchParameters := []byte(`{"timestamp":{"$gt":0},"timestamp":{"$lt":0}}`)

		x := DonateItemParams{}
		err := json.Unmarshal(searchParameters, &x)
		Expect(err).ToNot(HaveOccurred())

		_, err = ItemDonateReport(x, mgTable)
		Expect(err).To(HaveOccurred())
	})

	It("Error when $gt timestamp is empty", func() {
		searchParameters := []byte(`{"timestamp":{"$gt":0},"timestamp":{"$lt":10}}`)

		x := DonateItemParams{}
		err := json.Unmarshal(searchParameters, &x)
		Expect(err).ToNot(HaveOccurred())

		_, err = ItemDonateReport(x, mgTable)
		Expect(err).To(HaveOccurred())
	})

	It("Error when $lt timestamp is empty", func() {
		searchParameters := []byte(`{"timestamp":{"$gt":10},"timestamp":{"$lt":0}}`)

		x := DonateItemParams{}
		err := json.Unmarshal(searchParameters, &x)
		Expect(err).ToNot(HaveOccurred())

		_, err = ItemDonateReport(x, mgTable)
		Expect(err).To(HaveOccurred())
	})

	It("Insert into report the avgSold results", func() {
		searchParameters := []byte(`{"timestamp":{"$gt":9},"timestamp":{"$lt":21}}`)

		donateItemParams := DonateItemParams{}
		err := json.Unmarshal(searchParameters, &donateItemParams)
		Expect(err).ToNot(HaveOccurred())

		avgDonateReport, err := ItemDonateReport(donateItemParams, mgTable)
		Expect(err).ToNot(HaveOccurred())

		var reportAgg []ReportResult

		for _, v := range avgDonateReport {
			m, assertOK := v.(map[string]interface{})
			Expect(assertOK).To(BeTrue())

			avgSold, assertOK := m["avg_donate"].(float64)
			Expect(assertOK).To(BeTrue())
			log.Println(avgSold)
			if avgSold == item1.Weight {
				Expect(avgSold).To(Equal(item1.Weight))
				log.Println(item1.Weight)
			} else {
				Expect(avgSold).To(Equal(item2.Weight))
				log.Println(item2.Weight)
			}

			log.Println("$$$$$$$$$$$$$$$$$$", avgDonateReport)

			getIdMap := m["_id"]

			getInfoFromMap := getIdMap.(map[string]interface{})
			sku := getInfoFromMap["sku"].(string)
			name := getInfoFromMap["name"].(string)

			reportAgg = []ReportResult{
				ReportResult{
					Name:         name,
					SKU:          sku,
					DonateWeight: m["avg_donate"].(float64),
					TotalWeight:  m["avg_total"].(float64),
				},
			}
		}

		createTestDatabase("reportTest", &DonateReport{})
		reportID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		reportGen := DonateReport{
			ReportID:     reportID,
			SearchQuery:  donateItemParams,
			ReportResult: reportAgg,
		}

		_, err = CreateReport(reportGen, mgTable)
		Expect(err).ToNot(HaveOccurred())

		var findResults []interface{}
		findResults, err = mgTable.Find(map[string]interface{}{
			"reportID": map[string]interface{}{
				"$eq": reportID,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		for _, v := range findResults {
			log.Println(v.(*DonateReport))

			m := v.(*DonateReport)

			log.Println(m.ReportID.String())
			Expect(m.ReportID.String()).To(Equal(reportID.String()))
		}
	})

})
