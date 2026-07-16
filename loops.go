package main

import (
	"slices"
	"strconv"
	"time"

	"github.com/CyCoreSystems/ari/v6"
	"golang.org/x/exp/slog"
)

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
        if v.Channel.Caller != nil {
          id, err := strconv.Atoi(v.Channel.Caller.Number) 
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
    }
  }
}

func (app *Application) timerLoop() {
  ticker := time.NewTicker(1 * time.Minute)

  defer ticker.Stop()

  slog.Info("DBの監視を開始したのです")

  for {
    select {
    case <- ticker.C:
      slog.Info("DBの定期チェックなのです")
      app.checkReservedCalls()   
    }
  }
}

func (app *Application) checkReservedCalls() {
  var calls []ReservedCall
  now := time.Now()

  err := app.db.Where("run_at <= ?", now).Find(&calls).Error
  if err != nil {
    slog.Error("DBのSELECTに失敗しちゃったのです", "error", err)
    return
  }

  for _, call := range calls {
    slog.Info("予約されていた発信を実行するのですよ", "calleeID", call.CalleeID, "runAt", call.RunAt)

    // 発信をするのです
    app.Originate(call.CalleeID)
    
    // 発信をしたのでdbから消し飛ばすのです
    err := app.db.Delete(&call).Error
    if err != nil {
      slog.Error("DBからレコードの削除に失敗したみたいなのです", "error", err)
      continue
    } else {
      slog.Info("DBからレコードを削除したのです", "ID", call.ID)
    }
  }
  
}
