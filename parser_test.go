package kafkamysql

import (
  "testing";

  "github.com/stretchr/testify/assert";
)

func TestParseJson(t *testing.T) {
  events := [][]byte{
    []byte(`{"foo": "bar", "amount": 100}`),
    []byte(`{"foo": "baz", "amount": 200}`),
  }

  expected := []map[string]interface{}{
    map[string]interface{}{"foo": "bar", "amount": float64(100)},
    map[string]interface{}{"foo": "baz", "amount": float64(200)},
  }

  parsed, err := ParseJson(events)
  assert.Nil(t, err)
  assert.Equal(t, expected, parsed)
}