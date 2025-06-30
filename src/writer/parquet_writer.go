package writer

import (
    "os"
    "github.com/xitongsys/parquet-go-source/local"
    "github.com/xitongsys/parquet-go/writer"
)

func WriteParquet(outputPath string, data []map[string]interface{}) error {
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
