package main

import (
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/exp/slog"

	"github.com/CyCoreSystems/ari/v6"
	"github.com/CyCoreSystems/ari/v6/client/native"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// DBの構造体なのです
type ReservedCall struct {
	gorm.Model
	CalleeID	int				`gorm:"not null"`
	RunAt 		time.Time	`gorm:"not null;index"`
}

type Application struct {
	cl	ari.Client
	db	*gorm.DB
}

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

  slog.Info("終了するときは^Cを送信してほしいのですよ")

  // ^Cが送信されたら終了するのです
  sig := make(chan os.Signal, 1)
  signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
  <-sig
}

func (app *Application) eventLoop() {
  // イベントを待機するのです
  sub := app.cl.Bus().Subscribe(nil, "StasisStart", "StasisEnd", "ChannelDtmfReceived")
  defer sub.Cancel()
  slog.Info("イベントを待機しているのですよ")

  pushedButtonsMaps := make(map[string][]string)
  for e := range sub.Events() {
    switch v := e.(type) {
    case *ari.StasisStart:
      isOutbound := slices.Contains(v.Args, "outbound")
      
      if isOutbound {
        slog.Info("こちら側からの通話に出たみたいなのですよ", "channelID", v.Channel.ID)
      } else {
        slog.Info("向こう側から通話がかかってきたのですよ", "channelID", v.Channel.ID)

        h := app.cl.Channel().Get(ari.NewKey(ari.ChannelKey, v.Channel.ID))
        err := h.Answer()
        if err != nil {
          slog.Error("応答に失敗しちゃったのです", "error", err)
        } else {
					slog.Info("相手が応答してくれたのです", "channelID", v.Channel.ID)
				}
      }

    case *ari.StasisEnd:
      slog.Info("通話が切れたみたいなのですよ", "channelID", v.Channel.ID)

    case *ari.ChannelDtmfReceived:
      digit := v.Digit
      // channelIDの履歴を呼び出すのです
      pushedButtons := pushedButtonsMaps[v.Channel.ID]
      slog.Info("ボタンが押されたのですよ", "channelID", v.Channel.ID,  "digit", digit)
      pushedButtons = append(pushedButtons, digit)
      
      if len(pushedButtons) >= 4 {
        slog.Debug("押されたボタンの確認なのです", "channelID", v.Channel.ID, "button0", pushedButtons[0],"button1", pushedButtons[1], "button2", pushedButtons[2],  "button3", pushedButtons[3], )
        
        // 入力された時間をintにするのです
        hour, err := strconv.Atoi(pushedButtons[0] + pushedButtons[1])
        if err != nil || hour < 0 || hour > 23 {
          slog.Error("calleeからの時間入力がおかしかったみたいなのです", "channelID", v.Channel.ID, "errorPos", "Hour")
          delete(pushedButtonsMaps, v.Channel.ID)
          continue
        } 
        minute, err := strconv.Atoi(pushedButtons[2] + pushedButtons[3])
        if err != nil || minute < 0 || minute > 59 {
          slog.Error("calleeからの時間入力がおかしかったみたいなのです", "channelID", v.Channel.ID, "errorPos", "minute")
          delete(pushedButtonsMaps, v.Channel.ID)
          continue
        }
        
        // intをtime.Timeに変換するのです
        now := time.Now()
        runAt := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.Local)
        if runAt.Before(now) {
          runAt = runAt.Add(24 * time.Hour)
        }
        
        // TODO 他局からの接続時の挙動の確認をするのです
        // TODO 他局からホップで接続された時の挙動を確認するのです
        calleeID := 0
        if v.Channel.Dialplan != nil {
          id, err := strconv.Atoi(v.Channel.Dialplan.Exten) 
          if err == nil {
            calleeID = id
          }
        }
        newCall := ReservedCall {
          CalleeID: calleeID,
          RunAt: runAt,
        }
        app.db.Create(&newCall)
        slog.Info("DBにデータを保存したのですよ", "CalleeID", newCall.CalleeID, "RunAt", newCall.RunAt)

        slog.Info("4回ボタンが押されたので通話を切断するのですよ")
        app.Hangup(v.Channel.ID)
        delete(pushedButtonsMaps, v.Channel.ID)
      }
      
      // channelIDの履歴を書き込むのです
      pushedButtonsMaps[v.Channel.ID] = pushedButtons
      
      // if digit == "9" {
      //   }
      // }
    }
  }
}

func (app *Application) Hangup(channelID string) {
  h := app.cl.Channel().Get(ari.NewKey(ari.ChannelKey, channelID))
  err := h.Hangup()
  if err != nil {
    slog.Error("Hungupに失敗しちゃったのです",  "error", err)
  }
}

func Originate(cl ari.Client, calleeID int) {
  // 1. こちらから発信（Originate）するのです
  req := ari.OriginateRequest{
    Endpoint: "PJSIP/" + strconv.Itoa(calleeID),
    App:      "ari-test", // 応答したときにこのアプリに接続するのです
    AppArgs:  "outbound",
    Timeout:  60,
    CallerID: "\"ari-test\" <3001>",
  }

  slog.Info("新しく発信するのですよ", "endpoint", req.Endpoint)
  handle, err := cl.Channel().Originate(nil, req)
  if err != nil {
    slog.Error("発信に失敗しちゃったのです", "error", err)
  } else {
    slog.Info("発信に成功したのですよ！", "channelID", handle.ID())
  }
}
