package writer

import (
	"analytics-in-go/src/processor"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func WriteParquet(outputPath string, data []processor.AggregatedRow) error {
	f, err := local.NewLocalFileWriter(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	pw, err := writer.NewJSONWriter("record", f, 4)
	if err != nil {
		return err
	}
	defer pw.WriteStop()

	for _, row := range data {
		err := pw.Write(row)
		if err != nil {
			return err
		}
	}

	return nil
}
