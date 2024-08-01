package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/yankeguo/rg"
)

func main() {
	var err error
	defer func() {
		if err == nil {
			return
		}
		log.Println("exited with error:", err.Error())
		os.Exit(1)
	}()
	defer rg.Guard(&err)

	var (
		optOutput      string
		optURL         string
		optRepository  string
		optUsername    string
		optPassword    string
		optPrefix      string
		optConcurrency int
	)

	flag.StringVar(&optOutput, "output", "output.jsonl", "output jsonl file")
	flag.StringVar(&optURL, "url", "", "nexus2 base url to fetch")
	flag.StringVar(&optRepository, "repository", "", "nexus2 repository to fetch")
	flag.StringVar(&optUsername, "username", "", "nexus2 username")
	flag.StringVar(&optPassword, "password", "", "nexus2 password")
	flag.StringVar(&optPrefix, "prefix", "/", "nexus2 artifact prefix to filter")
	flag.IntVar(&optConcurrency, "concurrency", 5, "concurrency to fetch")
	flag.Parse()

	if optURL == "" {
		err = errors.New("url is required")
		return
	}
	if optRepository == "" {
		err = errors.New("repository is required")
		return
	}
	if optConcurrency < 1 {
		optConcurrency = 1
	}

	client := resty.New().
		SetBaseURL(rg.Must(url.JoinPath(optURL, "service", "local", "repositories", optRepository, "content"))).
		SetHeader("Accept", "application/json").SetDisableWarn(true)

	if optUsername != "" {
		client = client.SetBasicAuth(optUsername, optPassword)
	}

	f := rg.Must(os.OpenFile(optOutput, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644))
	defer f.Close()

	err = walk(optPrefix, walkOptions{
		cli: client,
		enc: json.NewEncoder(f),
		dup: map[string]struct{}{},
	})
}

type walkOptions struct {
	cli *resty.Client
	enc *json.Encoder
	dup map[string]struct{}
}

func walk(relPath string, opts walkOptions) (err error) {
	relPath = strings.TrimPrefix(relPath, "/")

	log.Println("iterate:", relPath)

	type OutputItem struct {
		File string `json:"file"`
		Size int64  `json:"size"`
	}

	type Result struct {
		Data []struct {
			ResourceURI  string `json:"resourceURI"`
			RelativePath string `json:"relativePath"`
			Text         string `json:"text"`
			Leaf         bool   `json:"leaf"`
			LastModified string `json:"lastModified"`
			SizeOnDisk   int64  `json:"sizeOnDisk"`
		} `json:"data"`
	}
	var result Result

	var resp *resty.Response
	if resp, err = opts.cli.R().SetResult(&result).Get(relPath); err != nil {
		return
	}

	if resp.IsError() {
		err = errors.New(relPath + ": " + resp.String())
		return
	}

	for _, item := range result.Data {
		if item.Leaf {
			if item.SizeOnDisk < 0 {
				err = errors.New("sizeOnDisk < 0: " + item.ResourceURI)
				return
			} else if item.SizeOnDisk == 0 {
				log.Println("warning: sizeOnDisk == 0: " + item.ResourceURI)
			}

			if err = opts.enc.Encode(OutputItem{
				File: strings.TrimPrefix(item.RelativePath, "/"),
				Size: item.SizeOnDisk,
			}); err != nil {
				return
			}
		} else {
			if _, found := opts.dup[item.RelativePath]; found {
				continue
			}
			opts.dup[item.RelativePath] = struct{}{}
			if err = walk(item.RelativePath, opts); err != nil {
				return
			}
		}
	}

	return
}
