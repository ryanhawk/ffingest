package ffingest

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"time"

	"cloud.google.com/go/storage"
	"github.com/ryanhawk/nflv3stats"
)

var (
	ocp    = "Ocp-Apim-Subscription-Key"
	ocpkey = os.Getenv("OCPKEY")
	ctx    = context.TODO()
	cfg    = nflv3stats.NewConfiguration()
	format = "JSON"
)

func init() {

	cfg.AddDefaultHeader(ocp, ocpkey)

}

// IngestFFAPI endpoint for function
func IngestFFAPI(w http.ResponseWriter, r *http.Request) {
	ffClient := nflv3stats.NewAPIClient(cfg)
	var bx struct {
		Week     string `json:"week"`
		Season   string `json:"season"`
		HomeTeam string `json:"hometeam"`
		AwayTeam string `json:"awayteam"`
	}
	if err := json.NewDecoder(r.Body).Decode(&bx); err != nil {
		fmt.Fprint(w, "Please enter the appropriate values!")
	}
	f, file, sn := Boxscore(ctx, ffClient, bx.Week, bx.Season, bx.HomeTeam, bx.AwayTeam)
	fmt.Fprintf(w, "Here are the results: %s, %s, %s", f, file, sn)
	fmt.Fprintf(w, "Here is week and season: %s, %s, %s, %s!", bx.Week, bx.Season, bx.HomeTeam, bx.AwayTeam)
}

// Boxscore calls corresponding fantasydata api which returns boxscore struct
func Boxscore(ctx context.Context, client *nflv3stats.APIClient, week, season, hometeam, awayteam string) (folderDir, files, folderName []string) {

	boxscorev3, res, err := client.DefaultApi.BoxScoreV(ctx, format, season, week, hometeam)
	fmt.Println("I just called the boxscore API: ", boxscorev3)
	if err != nil {
		log.Printf("Issue with call to boxscorev: %v", err)
	}
	folderDir, files, folderName = parseBoxScore(boxscorev3, *res, season, week, hometeam, awayteam)

	return
}

func parseBoxScore(boxscorev3 nflv3stats.BoxScoreV3, res http.Response, season, week, hometeam, awayteam string) (folderDirectory, files, bxFolders []string) {
	fmt.Println("response code: ", res.StatusCode)

	// Parsing Score
	folders := "boxscore_score"
	bxFolders = append(bxFolders, folders)
	apiLabels := "boxscore_score" + hometeam + "_" + awayteam
	scoreDir, scoreFile := Parser(apiLabels, folders, week, season, hometeam, *boxscorev3.Score)
	folderDirectory = append(folderDirectory, scoreDir)
	files = append(files, scoreFile)

	// Parsing Quarters
	folderq := "boxscore_quarters"
	bxFolders = append(bxFolders, folderq)
	apiLabelq := "boxscore_quarters" + hometeam + "_" + awayteam
	quarterDir, quarterFile := Parser(apiLabelq, folderq, week, season, hometeam, boxscorev3.Quarters)
	folderDirectory = append(folderDirectory, quarterDir)
	files = append(files, quarterFile)

	// Parsing TeamGames
	foldertg := "boxscore_teamgames"
	bxFolders = append(bxFolders, foldertg)
	apiLabeltg := "boxscore_teamgames" + hometeam + "_" + awayteam
	tgDir, tgFile := Parser(apiLabeltg, foldertg, week, season, hometeam, boxscorev3.TeamGames)
	folderDirectory = append(folderDirectory, tgDir)
	files = append(files, tgFile)

	// Parsing PlayerGames
	folderpg := "boxscore_playergames"
	bxFolders = append(bxFolders, folderpg)
	apiLabelpg := "boxscore_playergames" + hometeam + "_" + awayteam
	pgDir, pgFile := Parser(apiLabelpg, folderpg, week, season, hometeam, boxscorev3.PlayerGames)
	folderDirectory = append(folderDirectory, pgDir)
	files = append(files, pgFile)

	// Parsing FantasyDefenseGames
	folderfdg := "boxscore_fantasydefensegames"
	bxFolders = append(bxFolders, folderfdg)
	apiLabelfdg := "boxscore_fantasydefensegames_" + hometeam + "_" + awayteam
	fdgDir, fdgFile := Parser(apiLabelfdg, folderfdg, week, season, hometeam, boxscorev3.FantasyDefenseGames)
	folderDirectory = append(folderDirectory, fdgDir)
	files = append(files, fdgFile)

	// Parsing ScoringPlay
	foldersp := "boxscore_scoringplays"
	bxFolders = append(bxFolders, foldersp)
	apiLabelsp := "boxscore_scoringplays_" + hometeam + "_" + awayteam
	bxsp := "ScoringPlayID"
	spDir, spFile := Parser(apiLabelsp, foldersp, week, season, hometeam, boxscorev3.ScoringPlays, bxsp)
	folderDirectory = append(folderDirectory, spDir)
	files = append(files, spFile)

	// Parsing ScoringDetails
	foldersd := "boxscore_scoringdetails"
	bxFolders = append(bxFolders, foldersd)
	apiLabelsd := "boxscore_scoringdetails_" + hometeam + "_" + awayteam
	sdDir, sdFile := Parser(apiLabelsd, foldersd, week, season, hometeam, boxscorev3.ScoringDetails)
	folderDirectory = append(folderDirectory, sdDir)
	files = append(files, sdFile)

	return

}

// Parser accepts apiObject (api endpoint name), week, season, hometeam and []object
func Parser(apiLabel string, folder string, week string, season string, hometeam string, args ...interface{}) (dir, fileProcessed string) {
	var header []string
	var records []string
	var csv [][]string
	var key string
	fulldir := season + "/" + week + "/" + folder + "/"
	fileName := fulldir + apiLabel + "_" + season + "_" + week + ".csv"
	for _, val := range args {
		valueOf := reflect.ValueOf(val)
		kind := valueOf.Kind()

		if kind.String() == "string" {
			key = valueOf.String()
			fmt.Println("key: ", key)
		}
		if kind.String() == "struct" {
			getIt := valueOf.Type()

			numfields := getIt.NumField()

			for i := 0; i < numfields; i++ {
				c := getIt.Field(i).Name
				header = append(header, c)
			}
			//Adding logic to add date processed field to header:
			dateprocessed := "date_processed"
			header = append(header, dateprocessed)
			csv = append(csv, header)

			for i := 0; i < numfields; i++ {
				value := valueOf.Field(i)
				records = append(records, fmt.Sprintf("%v", value))
			}
			currentTime := time.Now()
			dateTime := currentTime.Format("2006-01-02T15:04:05")
			records = append(records, dateTime)
			csv = append(csv, records)

		}

		if kind.String() == "slice" {
			length := valueOf.Len()
			getIt := valueOf.Type()
			numfields := getIt.Elem().NumField()

			for i := 0; i < numfields; i++ {
				c := getIt.Elem().Field(i).Name
				header = append(header, c)
			}
			dateprocessed := "date_processed"
			header = append(header, dateprocessed)
			csv = append(csv, header)

			for i := 0; i < length; i++ {
				value := valueOf.Index(i)

				for i := 0; i < numfields; i++ {
					records = append(records, fmt.Sprintf("%v", value.Field(i)))
				}
				currentTime := time.Now()
				dateTime := currentTime.Format("2006-01-02T15:04:05")
				records = append(records, dateTime)
				csv = append(csv, records)
				records = nil
			}

		}

	}

	dir, fileProcessed = writeCSVGCS(csv, fulldir, fileName)
	return dir, fileProcessed
}

func writeCSVGCS(data [][]string, directory string, filename string) (dir string, fileProcessed string) {

	tmpFile, err := ioutil.TempFile(os.TempDir(), "prefix-")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}

	// Remember to clean up the file afterwards
	defer os.Remove(tmpFile.Name())

	fmt.Println("Created File: " + tmpFile.Name())
	writer := csv.NewWriter(tmpFile)
	defer writer.Flush()

	error := writer.WriteAll(data)
	if error != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		fmt.Println("Issue opening file: ", err)
	}
	defer f.Close()
	//get the file size
	stat, err := f.Stat()
	if err != nil {
		fmt.Println("error with f.Stat: ", err)
	}
	fmt.Println("File size is: ", stat.Size())
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Println("Issue with client in writeCSVGCS: ", err)
	}

	bucket := os.Getenv("GOOG_BUCKET")
	destinationString := "fantasydata/" + filename

	wc := client.Bucket(bucket).Object(destinationString).NewWriter(ctx)
	if _, err := io.Copy(wc, f); err != nil {
		fmt.Println("Problem processing file: ", err)
	}
	if err := wc.Close(); err != nil {
		fmt.Println("Problem closing writer: ", err)
	}

	if err := tmpFile.Close(); err != nil {
		log.Fatal(err)
	}
	gcsfileProcessed := filename

	return directory, gcsfileProcessed

}
func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}
