package parser

import (
	"encoding/hex"
	"fmt"
	"tur/pkg/types"
)

// parseBlobLiteral parses a BLOB literal (hex string)
func (p *Parser) parseBlobLiteral() (*Literal, error) {
	decoded, err := hex.DecodeString(p.cur.Literal)
	if err != nil {
		return nil, fmt.Errorf("invalid blob literal: %v", err)
	}
	return &Literal{Value: types.NewBlob(decoded)}, nil
}
