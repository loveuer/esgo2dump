package core

import (
	"github.com/loveuer/esgo2dump/pkg/model"
	"github.com/spf13/cobra"
)

func RunSetting(cmd *cobra.Command, input model.IO[map[string]any], output model.IO[map[string]any]) error {
	setting, err := input.ReadSetting(cmd.Context())
	if err != nil {
		return err
	}

	if err = output.WriteSetting(cmd.Context(), setting); err != nil {
		return err
	}

	return nil
}
