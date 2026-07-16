package main

import (
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"golang.org/x/exp/slog"

	"github.com/CyCoreSystems/ari/v6"
	"github.com/CyCoreSystems/ari/v6/client/native"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)


func main() {
  // デバッグ用のログを出力するためのロガーを作るのですよ
  logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
  slog.SetDefault(logger)
  
  cl, err := connectARI(logger)
  if err != nil {
    slog.Error("ARIの接続に失敗しちゃったのです", "error", err)
    os.Exit(1)
  }
  db, err := connectDB()
  if err != nil {
    slog.Error("DBの接続に失敗しちゃったのです", "error", err)
    os.Exit(1)
  }

  app := &Application{
    cl: cl,
    db: db,
  }

  app.Run()
}

func connectARI(logger *slog.Logger) (ari.Client, error){
  // AsteriskのARIに接続するための設定なのです
  cl, err := native.Connect(&native.Options{
    Application:  "ari-test",
    Username:     "test",
    Password:     "testtest",
    URL:          "http://kurodenpbxtype1.tailfcad4e.ts.net:8088/asterisk/ari",
    WebsocketURL: "ws://kurodenpbxtype1.tailfcad4e.ts.net:8088/asterisk/ari/events",
    Logger:        logger,
  })
  if err != nil {
    return nil, err
  }
  return cl, nil
}

func connectDB() (*gorm.DB, error) {
	// dbに接続するのです
	db, err := gorm.Open(sqlite.Open("calls.db"), &gorm.Config{})
  if err != nil {
    return nil, err
  }
	db.AutoMigrate(&ReservedCall{}) // おーとまいぐれーしょん なのです 
  return db, nil
}

func (app *Application) Run() {
  // イベント待機用のゴルーチンを動かすのです
  slog.Info("接続できたのですよ！")
  go app.eventLoop()

  // DB確認用のゴルーチンを動かすのです
  go app.timerLoop()

  slog.Info("終了するときは^Cを送信してほしいのですよ")

  // ^Cが送信されたら終了するのです
  sig := make(chan os.Signal, 1)
  signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
  <-sig
}

func (app *Application) Hangup(channelID string) {
  h := app.cl.Channel().Get(ari.NewKey(ari.ChannelKey, channelID))
  err := h.Hangup()
  if err != nil {
    slog.Error("Hungupに失敗しちゃったのです",  "error", err)
  }
}

func (app *Application) Originate(calleeID int) {
  // 1. こちらから発信（Originate）するのです
  req := ari.OriginateRequest{
    Endpoint: "PJSIP/" + strconv.Itoa(calleeID),
    App:      "ari-test", // 応答したときにこのアプリに接続するのです
    AppArgs:  "outbound",
    Timeout:  60,
    CallerID: "\"ari-test\" <3001>",
  }

  slog.Info("新しく発信するのですよ", "endpoint", req.Endpoint)
  handle, err := app.cl.Channel().Originate(nil, req)
  if err != nil {
    slog.Error("発信に失敗しちゃったのです", "error", err)
  } else {
    slog.Info("発信に成功したのですよ！", "channelID", handle.ID())
  }
}
