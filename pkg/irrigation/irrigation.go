package irrigation

type IrrigationEvent struct {
	ID                 string `json:"id" dynamodbav:"id"`
	ReportTime         string `json:"reportTime,omitempty" dynamodbav:"report_time,omitempty"`
	Location           string `json:"location,omitempty" dynamodbav:"location,omitempty"`
	IrrigationDuration int    `json:"irrigationDuration,omitempty" dynamodbav:"duration,omitempty"`
}

type Irrigations []*IrrigationEvent

type IrrigationsLookup map[string]*IrrigationEvent
