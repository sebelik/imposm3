package gcs

type FieldType interface {
	AvroType() AvroType
}

type simpleFieldType struct {
	fieldType AvroType
}

func (t *simpleFieldType) AvroType() AvroType {
	return t.fieldType
}

type geometryType struct {
	fieldType AvroType
}

func (t *geometryType) AvroType() AvroType {
	return AvroTypeString
}

type validatedGeometryType struct {
	geometryType
}

type tagsType struct{}

func (t *tagsType) AvroType() AvroType {
	return AvroTypeRecord
}

var avroTypes map[string]FieldType

func init() {
	avroTypes = map[string]FieldType{
		"string":             &simpleFieldType{AvroTypeString},
		"bool":               &simpleFieldType{AvroTypeBool},
		"int8":               &simpleFieldType{AvroTypeInt},
		"int32":              &simpleFieldType{AvroTypeInt},
		"int64":              &simpleFieldType{AvroTypeLong},
		"float32":            &simpleFieldType{AvroTypeFloat},
		"hstore_string":      &tagsType{},
		"geometry":           &geometryType{AvroTypeBytes},
		"validated_geometry": &validatedGeometryType{geometryType{AvroTypeBytes}},
	}
}
