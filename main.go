package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/coreos/go-systemd/dbus"

	client "github.com/influxdata/influxdb1-client"
)

func main() {

	conn, err := dbus.New()

	var influxClient *client.Client = initInflux()

	// Ensuring that the connection is closed at some point.
	defer conn.Close()

	if err != nil {
		fmt.Println("Could not create a new connection object.")
		return
	}

	// Subscribing to systemd dbus events.
	err = conn.Subscribe()

	if err != nil {
		fmt.Println("Could not subscribe to the bus.")
		return
	}

	updateCh := make(chan *dbus.PropertiesUpdate, 256)
	errCh := make(chan error, 256)

	// Properties (signals here) changes will be saved to those objects.
	conn.SetPropertiesSubscriber(updateCh, errCh)

	for {
		select {
		case update := <-updateCh:

			var points []client.Point = getPoint(update)

			influxClient.Write(client.BatchPoints{
				Points:           points,
				Database:         "systemd",
				RetentionPolicy:  "autogen",
				Precision:        "ms",
				WriteConsistency: "any",
			})

		case err := <-errCh:
			fmt.Println(err)
		}
	}
}

func getPoint(properties *dbus.PropertiesUpdate) []client.Point {

	activeState := properties.Changed["ActiveState"].String()

	var point client.Point = client.Point{
		Measurement: "services",
		Tags: map[string]string{
			"service": properties.UnitName,
		},
		Fields: map[string]interface{}{
			"state": activeState,
			"value": getStateValue(activeState),
		},
		// Need to use the timestamp provided by dbus..
		Time:      time.Now(),
		Precision: "ms",
	}

	points := [1]client.Point{point}

	return points[0:1]

}

func initInflux() *client.Client {

	host, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	if err != nil {
		log.Fatal(err)
	}

	conf := client.Config{
		URL:      *host,
		Username: os.Getenv("admin"),
		Password: os.Getenv("admin"),
	}

	influxConnection, err := client.NewClient(conf)

	if err != nil {
		fmt.Println("Error building a new client for InfluxDB.")
	}

	return influxConnection
}

// active (1), reloading, inactive (0), failed (-1), activating, deactivating
func getStateValue(state string) int {

	switch state {
	case "\"failed\"":
		return -1
	case "\"inactive\"":
		return 0
	case "\"active\"":
		return 1
	case "\"reloading\"":
		return 2
	case "\"activating\"":
		return 4
	case "\"deactivating\"":
		return 5
	default:
		panic("Unhandled dbus active state value")
	}
}
