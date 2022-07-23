package avro

type FieldType interface {
	AvroType() AvroType
}

type SimpleFieldType struct {
	fieldType AvroType
}

func (t *SimpleFieldType) AvroType() AvroType {
	return t.fieldType
}

type GeometryType struct {
	fieldType AvroType
}

func (t *GeometryType) AvroType() AvroType {
	return AvroTypeString
}

type ValidatedGeometryType struct {
	GeometryType
}

type TagsType struct{}

func (t *TagsType) AvroType() AvroType {
	return AvroTypeRecord
}

var avroTypes map[string]FieldType

func init() {
	avroTypes = map[string]FieldType{
		"string":             &SimpleFieldType{AvroTypeString},
		"bool":               &SimpleFieldType{AvroTypeBool},
		"int8":               &SimpleFieldType{AvroTypeInt},
		"int32":              &SimpleFieldType{AvroTypeInt},
		"int64":              &SimpleFieldType{AvroTypeLong},
		"float32":            &SimpleFieldType{AvroTypeFloat},
		"hstore_string":      &TagsType{},
		"geometry":           &GeometryType{AvroTypeBytes},
		"validated_geometry": &ValidatedGeometryType{GeometryType{AvroTypeBytes}},
	}
}
