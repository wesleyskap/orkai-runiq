package queue

import (
	"strconv"
	"strings"
	"time"
)

// MatchCron returns true if the time matches the 5-field cron spec.
func MatchCron(spec string, t time.Time) bool {
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return false
	}
	return matchMinute(fields[0], t.Minute()) &&
		matchHour(fields[1], t.Hour()) &&
		matchDom(fields[2], t.Day()) &&
		matchMonth(fields[3], int(t.Month())) &&
		matchDow(fields[4], int(t.Weekday()))
}

func matchMinute(spec string, val int) bool {
	return matchPart(spec, val, 0, 59)
}

func matchHour(spec string, val int) bool {
	return matchPart(spec, val, 0, 23)
}

func matchDom(spec string, val int) bool {
	return matchPart(spec, val, 1, 31)
}

func matchMonth(spec string, val int) bool {
	return matchPart(spec, val, 1, 12)
}

func matchDow(spec string, val int) bool {
	if val == 0 {
		return matchPart(spec, 0, 0, 7) || matchPart(spec, 7, 0, 7)
	}
	return matchPart(spec, val, 0, 7)
}

func matchPart(spec string, val int, min, max int) bool {
	if spec == "*" {
		return true
	}
	if strings.Contains(spec, ",") {
		return matchList(spec, val, min, max)
	}
	if strings.HasPrefix(spec, "*/") {
		return matchStep(spec, val, min, max)
	}
	if strings.Contains(spec, "-") {
		return matchRange(spec, val)
	}
	return matchSingle(spec, val)
}

func matchList(spec string, val int, min, max int) bool {
	parts := strings.Split(spec, ",")
	for _, p := range parts {
		if matchPart(p, val, min, max) {
			return true
		}
	}
	return false
}

func matchStep(spec string, val int, min, max int) bool {
	stepStr := strings.TrimPrefix(spec, "*/")
	step, err := strconv.Atoi(stepStr)
	if err != nil || step <= 0 {
		return false
	}
	return (val-min)%step == 0
}

func matchRange(spec string, val int) bool {
	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return false
	}
	start, errStart := strconv.Atoi(parts[0])
	end, errEnd := strconv.Atoi(parts[1])
	if errStart != nil || errEnd != nil {
		return false
	}
	return val >= start && val <= end
}

func matchSingle(spec string, val int) bool {
	target, err := strconv.Atoi(spec)
	if err != nil {
		return false
	}
	return val == target
}
