package tsm1

/*
This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
the timestamp compression functionality.

It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
this version.
*/

import (
	"bytes"
	"fmt"
	"github.com/dgryski/go-bitstream"
	"math"
	"math/bits"
)

// Note: an uncompressed format is not yet implemented.
// floatCompressedGorilla is a compressed format using the gorilla paper encoding
const floatCompressedGorilla = 1

const previousValues = 128
var previousValuesLog2 =  int(math.Log2(previousValues))

// uvnan is the constant returned from math.NaN().
const uvnan = 0x7FF8000000000001

// FloatEncoder encodes multiple float64s into a byte slice.
type FloatEncoder struct {
	val [previousValues]float64
	i *index
	err error

	leading  uint64
	trailing uint64

	buf bytes.Buffer
	bw  *bitstream.BitWriter

	first    bool
	finished bool
	current  uint64
	index      uint64
	comparisonsCounter  uint64
}

// NewFloatEncoder returns a new FloatEncoder.
func NewFloatEncoder() *FloatEncoder {
	s := FloatEncoder{
		first:   true,
		leading: ^uint64(0),
		i:       createIndex(),

	}

	s.bw = bitstream.NewWriter(&s.buf)
	s.buf.WriteByte(floatCompressedGorilla << 4)

	return &s
}

// Reset sets the encoder back to its initial state.
func (s *FloatEncoder) Reset() {
	for i := 0; i < previousValues; i++ {
		s.val[i] = 0
	}
	s.i = createIndex()
	s.err = nil
	s.leading = ^uint64(0)
	s.trailing = 0
	s.comparisonsCounter = 0
	s.buf.Reset()
	s.buf.WriteByte(floatCompressedGorilla << 4)

	s.bw.Resume(0x0, 8)

	s.finished = false
	s.first = true
	s.current = 0
}

// Bytes returns a copy of the underlying byte buffer used in the encoder.
func (s *FloatEncoder) Bytes() ([]byte, error) {
	return s.buf.Bytes(), s.err
}

// Flush indicates there are no more values to encode.
func (s *FloatEncoder) Flush() {
	if !s.finished {
		// write an end-of-stream record
		s.finished = true
		s.Write(math.NaN())
		s.bw.Flush(bitstream.Zero)
	}
}


// Write encodes v to the underlying buffer.
func (s *FloatEncoder) Write(v float64) {
	//key := math.Float64bits(v) & uint64(0x1f)
	//_ = key
	// Only allow NaN as a sentinel value
	if math.IsNaN(v) && !s.finished {
		s.err = fmt.Errorf("unsupported value: NaN")
		return
	}
	if s.first {
		// first point
		s.val[s.current] = v
		s.i.addRecord(v, s.index)
		s.first = false
		//fmt.Printf("Value: %G, writing first as float64\n", s.val)
		s.bw.WriteBits(math.Float64bits(v), 64)
		return
	}
	previousIndex := s.i.getAll(v, s.index, previousValues)
	if previousIndex == previousValues {
		previousIndex = s.index % previousValues
    }

	vDelta := math.Float64bits(v) ^ math.Float64bits(s.val[previousIndex])

	/*f, err := os.OpenFile("/home/panagiotis/log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()*/

	if vDelta == 0 {
		//fmt.Printf("Value: %G, Delta = %064b, 1 bit (0)...\n", v, vDelta)
		s.bw.WriteBits(previousIndex * 2, previousValuesLog2 + 1)
		//f.WriteString(fmt.Sprintf("0,%d,%d\n", previousValuesLog2 + 1, 0))
		//s.bw.WriteBit(bitstream.Zero)
	} else {
		//s.bw.WriteBit(bitstream.One)

		leading := uint64(bits.LeadingZeros64(vDelta))
		trailing := uint64(bits.TrailingZeros64(vDelta))

		// Clamp number of leading zeros to avoid overflow when encoding
		leading &= 0x1F
		if leading >= 15 {
			leading = 14
		}

		s.leading, s.trailing = leading, trailing

		// Note that if leading == trailing == 0, then sigbits == 64.  But that
		// value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would
		// put us in the other case (vdelta == 0).  So instead we write out a 0 and
		// adjust it back to 64 on unpacking.
		sigbits := 64 - 2 * (leading / 2) - trailing
		if trailing < 6 {
			//s.bw.WriteBit(bitstream.Zero)
			//s.bw.WriteBits(previousIndex, previousValuesLog2)
			s.bw.WriteBits(previousIndex * 32 + 16 + leading / 2, previousValuesLog2 + 5)
			s.bw.WriteBits(vDelta, int(sigbits + trailing))
			//f.WriteString(fmt.Sprintf("1,%d,%d\n", previousValuesLog2 + 5, int(sigbits + trailing)))
			//fmt.Printf("%d, %d\n", previousValuesLog2 + 5, int(sigbits + trailing))
			//fmt.Printf("Value: %G, Delta = %064b, Case 2, 1 bit (1), 1 bit (0), leading (3 bits), vDelta (%v bits)\n", v, vDelta, sigbits + trailing)
		} else {
			//fmt.Printf("Value: %G, Delta = %064b, Case 2, 1 bit (1), 1 bit (1) leading (3 bits), sigbits (6 bits), vDelta>>trailing (%v bits)\n", v, vDelta, sigbits)
			//s.bw.WriteBit(bitstream.One)
			//s.bw.WriteBits(previousIndex, previousValuesLog2)
			s.bw.WriteBits((previousIndex * 32 + 16 + 8 + leading / 2) * 64 + sigbits, previousValuesLog2 + 6 + 5)
			//s.bw.WriteBits(sigbits, 6)
			s.bw.WriteBits(vDelta>>trailing, int(sigbits))
			//f.WriteString(fmt.Sprintf("2,%d,%d\n", previousValuesLog2 + 6 + 5, int(sigbits)))

			//fmt.Printf("%d, %d, %d\n", previousValuesLog2 + 5, 6, int(sigbits))
		}

	}

	s.current = (s.current + 1 ) % previousValues
	s.val[s.current] = v
	s.index = s.index + 1
	//fmt.Printf("Adding %v with %d\n", v, s.index)
	s.i.addRecord(v, s.index)
	//fmt.Printf("Total comparisons: %d\n", s.comparisonsCounter)
}

// Write2 encodes v to the underlying buffer.
func (s *FloatEncoder) Write2(v float64) {
	// Only allow NaN as a sentinel value
	if math.IsNaN(v) && !s.finished {
		s.err = fmt.Errorf("unsupported value: NaN")
		return
	}
	if s.first {
		// first point
		s.val[0] = v
		s.first = false
		//fmt.Printf("Value: %G, writing first as float64\n", s.val)
		s.bw.WriteBits(math.Float64bits(v), 64)
		return
	}

	vDelta := math.Float64bits(v) ^ math.Float64bits(s.val[0])

	if vDelta == 0 {
		//fmt.Printf("Value: %G, Delta = %064b, 1 bit (0)...\n", v, vDelta)
		s.bw.WriteBit(bitstream.Zero)
	} else {
		s.bw.WriteBit(bitstream.One)

		leading := uint64(bits.LeadingZeros64(vDelta))
		trailing := uint64(bits.TrailingZeros64(vDelta))

		// Clamp number of leading zeros to avoid overflow when encoding
		leading &= 0x1F
		if leading >= 32 {
			leading = 31
		}

		// TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
		if s.leading != ^uint64(0) && leading >= s.leading && trailing >= s.trailing {
			//fmt.Printf("Value: %G, Delta = %064b, Case 1, 1 bit (1), 1 bit (0), vDelta>>s.trailing (%v bits)\n", v, vDelta, 64-int(s.leading)-int(s.trailing))
			s.bw.WriteBit(bitstream.Zero)
			s.bw.WriteBits(vDelta>>s.trailing, 64-int(s.leading)-int(s.trailing))
		} else {
			s.leading, s.trailing = leading, trailing

			s.bw.WriteBit(bitstream.One)
			s.bw.WriteBits(leading, 5)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that
			// value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would
			// put us in the other case (vdelta == 0).  So instead we write out a 0 and
			// adjust it back to 64 on unpacking.
			sigbits := 64 - leading - trailing
			//fmt.Printf("Value: %G, Delta = %064b, Case 2, 1 bit (1), 1 bit (1), leading (5 bits), sigbits (6 bits), vDelta>>trailing (%v bits)\n", v, vDelta, sigbits)
			s.bw.WriteBits(sigbits, 6)
			s.bw.WriteBits(vDelta>>trailing, int(sigbits))
		}
	}

	s.val[0] = v
}

// FloatDecoder decodes a byte slice into multiple float64 values.
type FloatDecoder struct {
	val uint64
	values [previousValues]uint64

	leading  uint64
	trailing uint64
	current uint64

	br BitReader
	b  []byte

	first    bool
	finished bool

	err error
}

// SetBytes initializes the decoder with b. Must call before calling Next().
func (it *FloatDecoder) SetBytes(b []byte) error {
	var v uint64
	if len(b) == 0 {
		v = uvnan
	} else {
		// first byte is the compression type.
		// we currently just have gorilla compression.
		it.br.Reset(b[1:])

		var err error
		v, err = it.br.ReadBits(64)
		if err != nil {
			return err
		}
	}

	// Reset all fields.
	for i := 0; i < previousValues; i++ {
		it.values[i] = 0
	}
	it.val = v
	it.current = 0
	it.values[it.current] = v
	it.leading = 0
	it.trailing = 0
	it.b = b
	it.first = true
	it.finished = false
	it.err = nil

	return nil
}

// Next returns true if there are remaining values to read.
func (it *FloatDecoder) Next() bool {
	if it.err != nil || it.finished {
		return false
	}

	if it.first {
		it.first = false
		// mark as finished if there were no values.
		if it.values[it.current] == uvnan { // IsNaN
			it.finished = true
			return false
		}
		fmt.Printf("First value is ready\n")
		return true
	}

	// read index of previous value
	index, err := it.br.ReadBits(uint(previousValuesLog2))
	if err != nil {
		it.err = err
		return false
	}
	// read compressed value
	var bit bool
	if it.br.CanReadBitFast() {
		bit = it.br.ReadBitFast()
	} else if v, err := it.br.ReadBit(); err != nil {
		it.err = err
		return false
	} else {
		bit = v
	}
	if !bit {
		//fmt.Printf("Same value!\n")
		// it.val = it.val
		it.val = it.values[index]
		//fmt.Printf("Index: %d, Value: %64b\n", index, it.val)
		it.current = (it.current + 1) % previousValues
		it.values[it.current] = it.val
	} else {
		var bit bool
		if it.br.CanReadBitFast() {
			bit = it.br.ReadBitFast()
		} else if v, err := it.br.ReadBit(); err != nil {
			it.err = err
			return false
		} else {
			bit = v
		}

		if !bit {
			bits, err := it.br.ReadBits(3)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = bits * 2

			mbits := 64 - it.leading
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 0
		} else {
			bits, err := it.br.ReadBits(3)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = bits * 2

			bits, err = it.br.ReadBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := bits
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}

		mbits := uint(64 - it.leading - it.trailing)
		bits, err := it.br.ReadBits(mbits)
		if err != nil {
			it.err = err
			return false
		}

		//vbits := it.val
		vbits := it.values[index]
		vbits ^= (bits << it.trailing)

		if vbits == uvnan { // IsNaN
			it.finished = true
			return false
		}
		//fmt.Printf("Index: %d, Value: %64b\n", index, it.val)
		it.val = vbits
		it.current = (it.current + 1) % previousValues
		it.values[it.current] = vbits
	}

	return true
}

// Next2 returns true if there are remaining values to read.
func (it *FloatDecoder) Next2() bool {
	if it.err != nil || it.finished {
		return false
	}

	if it.first {
		it.first = false

		// mark as finished if there were no values.
		if it.val == uvnan { // IsNaN
			it.finished = true
			return false
		}

		return true
	}

	// read compressed value
	var bit bool
	if it.br.CanReadBitFast() {
		bit = it.br.ReadBitFast()
	} else if v, err := it.br.ReadBit(); err != nil {
		it.err = err
		return false
	} else {
		bit = v
	}

	if !bit {
		// it.val = it.val
	} else {
		var bit bool
		if it.br.CanReadBitFast() {
			bit = it.br.ReadBitFast()
		} else if v, err := it.br.ReadBit(); err != nil {
			it.err = err
			return false
		} else {
			bit = v
		}

		if !bit {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.ReadBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = bits

			bits, err = it.br.ReadBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := bits
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}

		mbits := uint(64 - it.leading - it.trailing)
		bits, err := it.br.ReadBits(mbits)
		if err != nil {
			it.err = err
			return false
		}

		vbits := it.val
		vbits ^= (bits << it.trailing)

		if vbits == uvnan { // IsNaN
			it.finished = true
			return false
		}
		it.val = vbits
	}

	return true
}

// Values returns the current float64 value.
func (it *FloatDecoder) Values() float64 {
	return math.Float64frombits(it.val)
}

// Error returns the current decoding error.
func (it *FloatDecoder) Error() error {
	return it.err
}
