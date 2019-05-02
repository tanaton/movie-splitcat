package main

import (
	"bufio"
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	SPLIT_FILE              = "split.txt"
	HTTP_TIMEOUT            = time.Second * 30
	COMMAND_TIMEOUT         = time.Minute * 30
	DOWNLOAD_PARALLEL       = 2
	CHUNK_DOWNLOAD_PARALLEL = 8
)

type Movie struct {
	vid          string
	start        time.Duration
	end          time.Duration
	udemae_start string
	udemae_end   string
}

var log *zap.SugaredLogger

func init() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	log = logger.Sugar()
}

func main() {
	sp := filepath.Join(".", SPLIT_FILE)
	if len(os.Args) >= 2 {
		sp = os.Args[1]
	}
	base := "."
	if len(os.Args) >= 3 {
		base = os.Args[2]
	}
	tmp := "."
	if len(os.Args) >= 4 {
		tmp = os.Args[3]
	}

	mm, err := readSplitFile(sp)
	if err != nil {
		log.Warnw("splitファイルの読み取りに失敗", "error", err)
		os.Exit(1)
	}
	log.Infow("splitファイルの読み取りに成功")

	ml, err := splitMovies(base, tmp, mm)
	if err != nil {
		log.Warnw("動画の切り出しに失敗", "error", err)
		os.Exit(1)
	}
	log.Infow("動画の切り出しに成功")

	cp, err := createConcatFile(tmp, ml)
	if err != nil {
		log.Warnw("concatファイルの生成に失敗", "error", err)
		os.Exit(1)
	}
	log.Infow("concatファイルの生成に成功")
	defer os.Remove(cp)
	err = ffmpegCombine(cp)
	if err != nil {
		log.Warnw("動画の生成に失敗", "error", err)
		os.Exit(1)
	}
	log.Infow("動画の生成に成功")
	os.Exit(0)
}

func readSplitFile(sp string) (map[string]Movie, error) {
	rfp, err := os.Open(sp)
	if err != nil {
		return nil, errors.Wrap(err, "分割ファイル（"+sp+"）が読み取れませんでした")
	}
	defer rfp.Close()
	mm := make(map[string]Movie, 32)
	r := bufio.NewScanner(rfp)
	for r.Scan() {
		line := r.Text()
		cell := strings.Split(line, "\t")
		if len(cell) < 5 {
			break
		}
		var m Movie
		m.vid = cell[0]
		m.start, err = time.ParseDuration(cell[1])
		if err != nil {
			return nil, errors.Wrap(err, "動画開始時刻がパースできませんでした：vid"+m.vid)
		}
		m.end, err = time.ParseDuration(cell[2])
		if err != nil {
			return nil, errors.Wrap(err, "動画終了時刻がパースできませんでした：vid"+m.vid)
		}
		m.udemae_start = cell[3]
		m.udemae_end = cell[4]
		if m.end == 0 {
			return nil, errors.New("動画終了時刻がゼロ：vid" + m.vid)
		}
		mm[m.vid] = m
	}
	return mm, nil
}

func splitMovies(base, tmp string, mm map[string]Movie) ([]string, error) {
	pl, err := filepath.Glob(filepath.Join(base, "*.mp4"))
	if err != nil {
		return nil, errors.Wrap(err, "フォルダの読み取りに失敗")
	}
	ml := []string{}
	sort.Strings(pl)
	for _, p := range pl {
		_, file := filepath.Split(p)
		i := strings.Index(file, "_")
		vid := file[0:i]
		if m, ok := mm[vid]; ok {
			mp := filepath.Join(tmp, m.vid+"_movie-splitcat.mp4")
			if isExist(mp) == false {
				err := m.ffmpegSplit(p, mp)
				if err != nil {
					return nil, errors.Wrap(err, "動画の切り出しに失敗")
				}
				log.Infow("動画の切り出しに成功", "vid", m.vid, "src", p, "dst", mp)
			} else {
				log.Infow("動画は切り出し済み", "vid", m.vid, "src", p, "dst", mp)
			}
			ml = append(ml, mp)
		}
	}
	return ml, nil
}

func createConcatFile(base string, ml []string) (string, error) {
	tfp, err := ioutil.TempFile(base, "_concat_")
	if err != nil {
		return "", err
	}
	defer tfp.Close()

	w := bufio.NewWriter(tfp)
	for _, it := range ml {
		filePath, _ := filepath.Abs(it)
		_, err := w.WriteString("file '" + filePath + "'\n")
		if err != nil {
			os.Remove(tfp.Name())
			return "", err
		}
	}
	err = w.Flush()
	if err != nil {
		os.Remove(tfp.Name())
		return "", err
	}

	return tfp.Name(), nil
}

func (m Movie) ffmpegSplit(src, dst string) error {
	ctx, cancel := context.WithTimeout(context.Background(), COMMAND_TIMEOUT)
	defer cancel()

	// ffmpeg -ss 4500 -i input.mp4 -t 60 -vcodec copy -acodec copy 1a.mp4
	args := []string{
		"-y",
		"-ss", strconv.FormatInt(int64(m.start.Seconds()), 10),
		"-i", src,
		"-t", strconv.FormatInt(int64(m.end.Seconds()-m.start.Seconds()), 10),
		"-c:v", "copy",
		"-c:a", "copy",
		dst,
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var sbuf strings.Builder
	cmd.Stderr = &sbuf
	err := cmd.Run()
	if err != nil {
		return err
	}
	return nil
}

func ffmpegCombine(cp string) error {
	ctx, cancel := context.WithTimeout(context.Background(), COMMAND_TIMEOUT)
	defer cancel()

	// ffmpeg -i input.mov -c:v libx264 -preset slow -crf 18 -c:a aac -b:a 192k -pix_fmt yuv420p output.mkv
	args := []string{
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", cp,
		"-threads", "12",
		"-preset", "veryfast",
		"-crf", "22",
		"-c:v", "libx264",
		"-c:a", "aac",
		"-b:a", "128k",
		"-r", "30",
		"-s", "1280x720",
		"-vsync", "1",
		"-deinterlace",
		"-pix_fmt", "yuv420p",
		"-bufsize", "20000k",
		"-maxrate", "2000k",
		"output.mkv",
	}
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	var sbuf strings.Builder
	cmd.Stderr = &sbuf
	err := cmd.Run()
	if err != nil {
		return errors.Wrap(err, sbuf.String())
	}
	return nil
}

func isExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}
