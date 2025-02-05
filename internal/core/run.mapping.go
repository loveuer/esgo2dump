package core

import (
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/spf13/cobra"
)

func RunMapping(cmd *cobra.Command, input model.IO[map[string]any], output model.IO[map[string]any]) error {
	mapping, err := input.ReadMapping(cmd.Context())
	if err != nil {
		return err
	}

	if err = output.WriteMapping(cmd.Context(), mapping); err != nil {
		return err
	}

	return nil
}
