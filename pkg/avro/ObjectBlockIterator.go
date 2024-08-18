package avro

type ObjectBlockIterator struct {
	reader       Reader
	expectedSync [16]byte
	err          error
}

type NewObjectBlockIteratorInput struct {
	Reader       Reader
	ExpectedSync [16]byte
}

func NewObjectBlockIterator(input NewObjectBlockIteratorInput) *ObjectBlockIterator {
	return &ObjectBlockIterator{
		reader:       input.Reader,
		expectedSync: input.ExpectedSync,
	}
}

func (objectBlockIterator *ObjectBlockIterator) Next(block *ObjectBlock) bool {
	if objectBlockIterator.err != nil {
		return false
	}
	ok, err := ReadObjectBlock(objectBlockIterator.reader, block, objectBlockIterator.expectedSync)
	if err != nil {
		objectBlockIterator.err = err
		return false
	}
	return ok
}

func (objectBlockIterator *ObjectBlockIterator) Err() error {
	return objectBlockIterator.err
}
