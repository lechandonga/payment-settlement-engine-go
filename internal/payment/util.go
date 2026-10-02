package payment

import "time"

func nowNs() int64 { return time.Now().UnixNano() }
