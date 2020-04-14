package typeconv

import (
	"fmt"
	"log"
	"reflect"
)

var (
	stringType = reflect.TypeOf("")
)

func Int(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case uint64:
		return int64(n)
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case uint32:
		return int64(n)
	case int16:
		return int64(n)
	case uint16:
		return int64(n)
	case int8:
		return int64(n)
	case uint8:
		return int64(n)
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	default:
		log.Panicf("Int: can not convert: %T %v", v, v)
		return 0
	}
}

func Float(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case uint32:
		return float64(n)
	case int16:
		return float64(n)
	case uint16:
		return float64(n)
	case int8:
		return float64(n)
	case uint8:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	default:
		log.Panicf("Float: can not convert: %T %v", v, v)
		return 0
	}
}

func IntTuple(v interface{}) []int64 {
	if t, ok := v.([]int64); ok {
		return t
	}
	if t, ok := v.([]int); ok {
		ret := make([]int64, len(t))
		for i, v := range t {
			ret[i] = int64(v)
		}
		return ret
	}

	if t, ok := v.([]interface{}); ok {
		ret := make([]int64, len(t))
		for i, v := range t {
			ret[i] = Int(v)
		}
		return ret
	}
	log.Panicf("IntTuple: can not convert: %T %v", v, v)
	return nil
}

func FloatTuple(v interface{}) []float64 {
	if t, ok := v.([]float64); ok {
		return t
	}
	if t, ok := v.([]float32); ok {
		ret := make([]float64, len(t))
		for i, v := range t {
			ret[i] = float64(v)
		}
		return ret
	}

	if t, ok := v.([]interface{}); ok {
		ret := make([]float64, len(t))
		for i, v := range t {
			ret[i] = Float(v)
		}
		return ret
	}

	log.Panicf("FloatTuple: can not convert: %T %v", v, v)
	return nil
}

func String(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	val := reflect.ValueOf(v)
	return val.Convert(stringType).Interface().(string)
}

func MapStringAnything(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}

	if m, ok := v.(map[interface{}]interface{}); ok {
		m2 := make(map[string]interface{}, len(m))
		for k, v := range m {
			m2[k.(string)] = v
		}
		return m2
	}

	log.Panicf("MapStringAnything: can not convert: %T %v", v, v)
	return nil
}

// try to convert value to target type, panic if fail
func Convert(val interface{}, targetType reflect.Type) reflect.Value {
	value := reflect.ValueOf(val)
	if !value.IsValid() {
		return reflect.Zero(targetType)
	}

	valType := value.Type()
	if valType.ConvertibleTo(targetType) {
		return value.Convert(targetType)
	}

	//fmt.Printf("Value type is %v, emptyInterfaceType is %v, equals %v\n", valType, emptyInterfaceType, valType == emptyInterfaceType)
	interfaceVal := value.Interface()

	switch realVal := interfaceVal.(type) {
	case float64:
		return reflect.ValueOf(realVal).Convert(targetType)
	case []interface{}:
		// val is of type []interface{}, try to convert to typ
		sliceSize := len(realVal)
		targetSlice := reflect.MakeSlice(targetType, 0, sliceSize)
		elemType := targetType.Elem()
		for i := 0; i < sliceSize; i++ {
			targetSlice = reflect.Append(targetSlice, Convert(value.Index(i), elemType))
		}
		return targetSlice
	}

	panic(fmt.Errorf("convert from type %v to %v failed: %v", valType, targetType, value))
}
