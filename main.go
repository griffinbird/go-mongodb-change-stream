package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
	"strings"

	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var IsMongoAtlas bool = false

type DbEvent struct {
	DocumentKey   documentKey `bson:"documentKey"`
	OperationType string      `bson:"operationType"`
}
type documentKey struct {
	ID primitive.ObjectID `bson:"_id"`
}
type result struct {
	ID         primitive.ObjectID `bson:"_id"`
	UserID     string             `bson:"userID"`
	ItemType   string             `bson:"itemType"`
	Brand      string             `bson:"brand"`
}

type requestStatistics struct {
	ActivityID                                  string
	CommandName                                 string
	EstimatedDelayFromRateLimitingInMilliseconds int
	RequestCharge                               float64
	RequestDurationInMilliseconds               int
	RetriedDueToRateLimiting                    bool
	OK                                          int
}

func main() {
	// waitGroup to wait for all goroutines launched here to finish
	var waitGroup sync.WaitGroup

	// Set client options and connect to MongoDB
	err := godotenv.Load(".env")
		if err != nil {
			log.Printf("error loading .env file")
		}
	
	client, err := mongo.NewClient(options.Client().ApplyURI(os.Getenv("MONGODB_URI")))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.TODO(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// check if using Mongo Atlas or Cosmos DB API for Mongo DB
	pattern := "mongodb.net"
	connStr := os.Getenv("MONGODB_URI")
	str_pos := strings.Index(connStr, pattern)
	if (str_pos > -1) {
		fmt.Printf("Connected to Mongo Atlas\n\n")
		IsMongoAtlas = true
	} else {
		fmt.Printf("Connected to Cosmos DB Mongo API\n\n")
		IsMongoAtlas = false
	}

	// Cleanup the connection when main function exists
	defer client.Disconnect(context.TODO())

	// set Mongodb database and collection name
	database := client.Database("change-stream-demo")
	collection := database.Collection("cart")

	// This will watch all any and all changes to the documents within the collection
	// and will be later used to iterate over indefinately
	//streamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup) <- this won't work with CDB.
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: bson.D{{Key: "$in", Value: []string{"update", "insert", "replace"}}}}}}},
		bson.D{{Key: "$project", Value: bson.D{
            {Key: "_id", Value: 1},
            {Key: "documentKey", Value: 1},        
            {Key: "fullDocument.userID", Value: 1},
            {Key: "fullDocument.itemType", Value: 1},
            {Key: "fullDocument.brand", Value: 1},
        }},
		},
	}
	//stream, err := collection.Watch(context.TODO(), mongo.Pipeline{}) <- this won't work with CDB.
	stream, err := collection.Watch(context.TODO(), pipeline, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		panic(err)
	}

	// Waitgroup counter
	waitGroup.Add(1)

	routineCtx, cancelFn := context.WithCancel(context.Background())
	_ = cancelFn

	// Watches collection in database and prints out any changed document
	// go-routine to make code non-blocking
	go listenToDBChangeStream(client, routineCtx, waitGroup, stream, collection, database)

	// Insert a MongoDB record every 5 seconds
	go insertRecord(client, database, collection)

	// Block until the WaitGroup counter goes back to 0; all the workers notified they’re done.
	waitGroup.Wait()
}

// function to insert data records to MongoDB collection
func insertRecord(client *mongo.Client, database *mongo.Database, collection *mongo.Collection) {
	// pre-populated values for ItemType and Brand
	ItemType := make([]string, 0)
	ItemType = append(
		ItemType,
		"T-Shirts",
		"Shoes",
		"Pants",
		"Socks",
		"Belt",
		"Watch",
		"Bracelet",
	)
	Brand := make([]string, 0)
	Brand = append(Brand, "Nike", "Addidas", "Puma", "Asics", "Mizuno")

	// insert new records to MongoDB every 5 seconds
	for {
		item := result{
			ID:         primitive.NewObjectID(),
			UserID:     strconv.Itoa(rand.Intn(10000)),
			ItemType: ItemType[rand.Intn(len(ItemType))],
			Brand:  Brand[rand.Intn(len(Brand))],
		}
		_, err := collection.InsertOne(context.TODO(), item)
		if err != nil {
			log.Fatal(err)
		}
		requestChargeValue := ""
		if !IsMongoAtlas {
			statistics, err := GetLastRequestStats(client, database)
			requestChargeValue = fmt.Sprintf("RequestCharge: %.2f %dms\n ", statistics.RequestCharge, statistics.RequestDurationInMilliseconds)
			if err != nil {
				log.Fatalf("%v",err)
				//return err
			}
			fmt.Printf("%v" + "   " + "%v", item, requestChargeValue)
		} else {
			fmt.Printf("%v\n", item)	
		}
		time.Sleep(3 * time.Second)
	}
}

func listenToDBChangeStream(
	client *mongo.Client,
	routineCtx context.Context,
	waitGroup sync.WaitGroup,
	stream *mongo.ChangeStream,
	collection *mongo.Collection,
	database *mongo.Database,
) {
	// Cleanup defer functions when this function exits
	defer stream.Close(routineCtx)
	// Wrap the worker call in a closure that makes sure to tell the WaitGroup that this worker is done
	defer waitGroup.Done()

	// Whenever there is a change in the collection, decode the change
	for stream.Next(routineCtx) {
		var DbEvent DbEvent
		if err := stream.Decode(&DbEvent); err != nil {
			log.Fatal(err)
		}
		// need this for Cosmso DB Mongo API doesn't return the OperationType
		if DbEvent.OperationType == "" {
			DbEvent.OperationType = "insert/update"
		}
		
		if DbEvent.OperationType == "insert" {
			fmt.Println("Insert operation detected")
		} else if DbEvent.OperationType == "insert/update" {
			fmt.Println("Insert/update operation detected")
		} else if DbEvent.OperationType == "update" {
			fmt.Println("Update operation detected")
		} else if DbEvent.OperationType == "delete" {
			fmt.Println("Delete operation detected : Unable to pull changes as its record is deleted")
		}

		// Print out the document that was inserted or updated
		if DbEvent.OperationType == "insert" || DbEvent.OperationType == "update" || DbEvent.OperationType == "insert/update" {
			// Find the mongodb document based on the objectID
			var result result
			err := collection.FindOne(context.TODO(), DbEvent.DocumentKey).Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			// Convert changd MongoDB document from BSON to JSON
			data, writeErr := bson.MarshalExtJSON(result, false, false)
			if writeErr != nil {
				log.Fatal(writeErr)
			}
			requestChargeValue := ""
			if !IsMongoAtlas {
				statistics, err := GetLastRequestStats(client, database)
				requestChargeValue = fmt.Sprintf("RequestCharge: %.2f %dms\n ", statistics.RequestCharge, statistics.RequestDurationInMilliseconds)
				if err != nil {
					log.Fatalf("%v",err)
					//return err
				}
				// Print the changed document in JSON format with Request Charge value
				fmt.Print(string(data) + "   ")
				fmt.Print(string(requestChargeValue))
				fmt.Println("")
			} else {
				// Print the changed document in JSON format
				fmt.Print(string(data))
				fmt.Println("\n")
			}
		}
	}
}

func GetLastRequestStats(client *mongo.Client, database *mongo.Database) (*requestStatistics, error) {
	ctx := context.TODO()

	statistics := requestStatistics{}
	map1 := map[string]interface{}{"getLastRequestStatistics": "1"}
	err := database.RunCommand(ctx, map1, nil).Decode(&statistics)
	if err != nil {
		return nil, err
	}
	return &statistics, nil
}