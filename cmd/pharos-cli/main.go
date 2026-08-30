package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/gasthecreator/pharos/pkg/consumer"
	"github.com/gasthecreator/pharos/pkg/query"
)

func printUsage() {
	fmt.Fprintf(os.Stderr, `pharos-cli: Query & inspection CLI for Pharos clinical adverse event pipeline (§2.3, §2.4, §5)

Usage:
  pharos-cli [flags] <command> <subcommand> [arguments]

Canonical Query Commands:
  query study <study_id> --from <RFC3339> --to <RFC3339>   Query adverse events for a trial in date range
  query site  <site_id>  [--min-seq <seq>]                  Query adverse events for a trial site
  query event <idempotency_key>                             Point lookup for a single adverse event

DLQ Inspection Commands:
  dlq list [--site <site_id>] [--limit <n>]                 List rejected events in dead-letter store
  dlq get  <idempotency_key>                                Inspect rejection details for a specific event

Flags:
  --hosts       Comma-separated Cassandra hosts (default: 127.0.0.1)
  --port        Cassandra port (default: 9042)
  --keyspace    Cassandra keyspace (default: pharos)
  --json        Output results in JSON format (default: false)
  --memory      Run with in-memory sample store for offline demo (default: false)

Examples:
  pharos-cli query study STUDY-001 --from 2026-08-01T00:00:00Z --to 2026-08-31T23:59:59Z
  pharos-cli query site SITE-US-01 --min-seq 1
  pharos-cli query event SITE-US-01:1
  pharos-cli dlq list --site SITE-US-01
  pharos-cli dlq get SITE-US-01:99
`)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Parse global flags before subcommands
	var (
		hostsFlag    string
		portFlag     int
		keyspaceFlag string
		jsonOutput   bool
		useMemory    bool
	)

	// Custom flag set for global options
	globalFlags := flag.NewFlagSet("pharos-cli", flag.ContinueOnError)
	globalFlags.StringVar(&hostsFlag, "hosts", "127.0.0.1", "Cassandra hosts")
	globalFlags.IntVar(&portFlag, "port", 9042, "Cassandra port")
	globalFlags.StringVar(&keyspaceFlag, "keyspace", "pharos", "Cassandra keyspace")
	globalFlags.BoolVar(&jsonOutput, "json", false, "Output in JSON format")
	globalFlags.BoolVar(&useMemory, "memory", false, "Use in-memory sample store")

	// Filter out global flags from args
	var remainingArgs []string
	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if strings.HasPrefix(arg, "-") {
			if strings.HasPrefix(arg, "--json") || arg == "-json" {
				jsonOutput = true
				continue
			}
			if strings.HasPrefix(arg, "--memory") || arg == "-memory" {
				useMemory = true
				continue
			}
		}
		remainingArgs = append(remainingArgs, arg)
	}

	if len(remainingArgs) == 0 {
		printUsage()
		os.Exit(1)
	}

	ctx := context.Background()

	// Initialize service
	var svc query.Service
	if useMemory {
		memSvc := query.NewMemoryService()
		seedSampleData(memSvc)
		svc = memSvc
	} else {
		cfg := query.DefaultCassandraServiceConfig()
		cfg.Hosts = strings.Split(hostsFlag, ",")
		cfg.Port = portFlag
		cfg.Keyspace = keyspaceFlag

		cSvc, err := query.NewCassandraService(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error connecting to Cassandra: %v\n(Hint: pass --memory to test offline with sample data)\n", err)
			os.Exit(1)
		}
		defer cSvc.Close()
		svc = cSvc
	}

	command := remainingArgs[0]
	switch command {
	case "query":
		handleQuery(ctx, svc, remainingArgs[1:], jsonOutput)
	case "dlq":
		handleDLQ(ctx, svc, remainingArgs[1:], jsonOutput)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func handleQuery(ctx context.Context, svc query.Service, args []string, jsonOutput bool) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	switch subcommand {
	case "study":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: pharos-cli query study <study_id> --from <RFC3339> --to <RFC3339>\n")
			os.Exit(1)
		}
		studyID := args[1]

		studyFlags := flag.NewFlagSet("query study", flag.ExitOnError)
		fromStr := studyFlags.String("from", "", "Start time in RFC3339 (e.g. 2026-08-01T00:00:00Z)")
		toStr := studyFlags.String("to", "", "End time in RFC3339 (e.g. 2026-08-31T23:59:59Z)")
		studyFlags.Parse(args[2:])

		now := time.Now().UTC()
		startTime := now.Add(-30 * 24 * time.Hour) // Default last 30 days
		endTime := now

		if *fromStr != "" {
			parsed, err := time.Parse(time.RFC3339, *fromStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid --from time format: %v\n", err)
				os.Exit(1)
			}
			startTime = parsed
		}
		if *toStr != "" {
			parsed, err := time.Parse(time.RFC3339, *toStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid --to time format: %v\n", err)
				os.Exit(1)
			}
			endTime = parsed
		}

		records, err := svc.GetEventsByStudy(ctx, studyID, startTime, endTime)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			out, _ := json.MarshalIndent(records, "", "  ")
			fmt.Println(string(out))
			return
		}

		printCanonicalRecordsTable(fmt.Sprintf("Adverse Events for Study: %s (Time Range: %s - %s)", studyID, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339)), records)

	case "site":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: pharos-cli query site <site_id> [--min-seq <n>]\n")
			os.Exit(1)
		}
		siteID := args[1]

		siteFlags := flag.NewFlagSet("query site", flag.ExitOnError)
		minSeq := siteFlags.Int64("min-seq", 1, "Minimum local sequence number")
		siteFlags.Parse(args[2:])

		records, err := svc.GetEventsBySite(ctx, siteID, *minSeq)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			out, _ := json.MarshalIndent(records, "", "  ")
			fmt.Println(string(out))
			return
		}

		printCanonicalRecordsTable(fmt.Sprintf("Adverse Events for Site: %s (Min Seq: %d)", siteID, *minSeq), records)

	case "event":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: pharos-cli query event <idempotency_key>\n")
			os.Exit(1)
		}
		idKey := args[1]

		rec, err := svc.GetEvent(ctx, idKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Event not found: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			out, _ := json.MarshalIndent(rec, "", "  ")
			fmt.Println(string(out))
			return
		}

		printCanonicalRecordDetail(rec)

	default:
		fmt.Fprintf(os.Stderr, "Unknown query subcommand: %s\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func handleDLQ(ctx context.Context, svc query.Service, args []string, jsonOutput bool) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	switch subcommand {
	case "list":
		dlqFlags := flag.NewFlagSet("dlq list", flag.ExitOnError)
		siteID := dlqFlags.String("site", "", "Filter by trial site ID")
		limit := dlqFlags.Int("limit", 50, "Max records to return")
		dlqFlags.Parse(args[1:])

		var records []*query.DLQRecord
		var err error

		if *siteID != "" {
			records, err = svc.ListDLQEventsBySite(ctx, *siteID, *limit)
		} else {
			records, err = svc.ListAllDLQEvents(ctx, *limit)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "DLQ query failed: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			out, _ := json.MarshalIndent(records, "", "  ")
			fmt.Println(string(out))
			return
		}

		title := "Dead-Letter Rejected Adverse Events"
		if *siteID != "" {
			title = fmt.Sprintf("Dead-Letter Rejected Events for Site: %s", *siteID)
		}
		printDLQRecordsTable(title, records)

	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Usage: pharos-cli dlq get <idempotency_key>\n")
			os.Exit(1)
		}
		idKey := args[1]

		rec, err := svc.GetDLQEvent(ctx, idKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "DLQ event not found: %v\n", err)
			os.Exit(1)
		}

		if jsonOutput {
			out, _ := json.MarshalIndent(rec, "", "  ")
			fmt.Println(string(out))
			return
		}

		printDLQRecordDetail(rec)

	default:
		fmt.Fprintf(os.Stderr, "Unknown dlq subcommand: %s\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printCanonicalRecordsTable(title string, records []*consumer.CanonicalRecord) {
	fmt.Printf("\n=== %s ===\n", title)
	if len(records) == 0 {
		fmt.Println("No records found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "IDEMPOTENCY_KEY\tSITE\tSEQ\tSTUDY\tEVENT_TIME\tSEVERITY\tMEDDRA\tSUBJECT\tLATE?")
	fmt.Fprintln(w, "---------------------------------------------------------------------------------------------------------")
	for _, r := range records {
		lateStr := "No"
		if r.IsLate {
			lateStr = "YES"
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			r.IdempotencyKey,
			r.SiteID,
			r.LocalSeq,
			r.StudyID,
			r.EventTime.Format("2006-01-02 15:04:05"),
			r.Severity,
			r.EventCode,
			r.Subject,
			lateStr,
		)
	}
	w.Flush()
	fmt.Printf("\nTotal Records: %d\n\n", len(records))
}

func printCanonicalRecordDetail(r *consumer.CanonicalRecord) {
	fmt.Printf("\n=== Adverse Event Record: %s ===\n", r.IdempotencyKey)
	fmt.Printf("Site ID:           %s\n", r.SiteID)
	fmt.Printf("Local Sequence:    %d\n", r.LocalSeq)
	fmt.Printf("Study ID:          %s\n", r.StudyID)
	fmt.Printf("Subject Reference: %s\n", r.Subject)
	fmt.Printf("Severity:          %s\n", r.Severity)
	fmt.Printf("MedDRA Code:       %s\n", r.EventCode)
	fmt.Printf("Event Time:        %s\n", r.EventTime.Format(time.RFC3339))
	fmt.Printf("Recorded Time:     %s\n", r.RecordedTime.Format(time.RFC3339))
	fmt.Printf("Ingestion Time:    %s\n", r.IngestionTime.Format(time.RFC3339))
	fmt.Printf("Consumed At:       %s\n", r.ConsumedAt.Format(time.RFC3339))
	fmt.Printf("Is Late Arrival:   %t\n", r.IsLate)
	fmt.Printf("Kafka Lineage:     topic=%s partition=%d offset=%d\n", r.KafkaTopic, r.KafkaPartition, r.KafkaOffset)
	fmt.Printf("\nRaw FHIR Payload:\n%s\n\n", r.Payload)
}

func printDLQRecordsTable(title string, records []*query.DLQRecord) {
	fmt.Printf("\n=== %s ===\n", title)
	if len(records) == 0 {
		fmt.Println("No dead-letter events found.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "IDEMPOTENCY_KEY\tSITE\tREJECTED_AT\tREJECTION_REASON\tSTATUS")
	fmt.Fprintln(w, "-----------------------------------------------------------------------------------------")
	for _, r := range records {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			r.IdempotencyKey,
			r.SiteID,
			r.RejectedAt.Format("2006-01-02 15:04:05"),
			r.RejectionReason,
			r.Status,
		)
	}
	w.Flush()
	fmt.Printf("\nTotal Rejected Events: %d\n\n", len(records))
}

func printDLQRecordDetail(r *query.DLQRecord) {
	fmt.Printf("\n=== Dead-Letter Event Details: %s ===\n", r.IdempotencyKey)
	fmt.Printf("Site ID:            %s\n", r.SiteID)
	fmt.Printf("Rejected At:        %s\n", r.RejectedAt.Format(time.RFC3339))
	fmt.Printf("Rejection Reason:   %s\n", r.RejectionReason)
	fmt.Printf("Status:             %s\n", r.Status)
	fmt.Printf("DLQ Kafka Lineage:  topic=%s partition=%d offset=%d\n", r.KafkaTopic, r.KafkaPartition, r.KafkaOffset)
	fmt.Printf("\nValidation Errors:\n  %s\n", strings.ReplaceAll(r.ValidationErrors, ";", "\n "))
	fmt.Printf("\nRejected Wire Payload:\n%s\n\n", r.Payload)
}

func seedSampleData(svc *query.MemoryService) {
	ctx := context.Background()
	baseTime := time.Now().UTC().Add(-2 * time.Hour)

	// Canonical event
	svc.CanonicalStore().SaveEvent(ctx, &consumer.CanonicalRecord{
		IdempotencyKey: "SITE-US-01:1",
		SiteID:         "SITE-US-01",
		StudyID:        "STUDY-001",
		LocalSeq:       1,
		EventTime:      baseTime,
		RecordedTime:   baseTime.Add(5 * time.Minute),
		IngestionTime:  baseTime.Add(6 * time.Minute),
		ConsumedAt:     baseTime.Add(7 * time.Minute),
		Severity:       "moderate",
		EventCode:      "10013661",
		Subject:        "Patient/US-101",
		Payload:        `{"resourceType":"AdverseEvent","actuality":"actual","subject":{"reference":"Patient/US-101"}}`,
		KafkaTopic:     "pharos.events.adverse",
		KafkaPartition: 0,
		KafkaOffset:    101,
		IsLate:         false,
	})

	// DLQ event
	svc.SaveDLQEvent(&query.DLQRecord{
		IdempotencyKey:   "SITE-US-01:99",
		SiteID:           "SITE-US-01",
		Payload:          `{"resourceType":"AdverseEvent","actuality":"invalid_status"}`,
		RejectionReason:  "FHIR validation failed",
		ValidationErrors: "invalid actuality 'invalid_status'; missing required event coding",
		RejectedAt:       baseTime.Add(15 * time.Minute),
		Status:           "PUBLISHED",
		KafkaTopic:       "pharos.events.dlq",
		KafkaPartition:   0,
		KafkaOffset:      15,
	})
}
