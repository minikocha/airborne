package provider

import (
	"errors"
	"fmt"
	"os"
)

// TODO: 同じタイミングで同じディレクトリを作成しようとするかもしれないのでリトライかMutexでの排他処理を追加するか検討する
func createDir(dir string) error {
	if f, err := os.Stat(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		// NOTE: dirで指定されたパスが存在しない以外のエラー（権限など）
		return err
	} else if err == nil && !f.IsDir() {
		// NOTE: dirで指定されたパスがファイルだった
		return fmt.Errorf("File exists: %s", dir)
	} else if err == nil {
		// NOTE: dirで指定されたパスがすでに存在する
		return err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return nil
}
