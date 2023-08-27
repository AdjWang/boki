package common

import (
	"testing"
)

func TestCompress(t *testing.T) {
	data := "{\"12519249207688416610\":\"{\\\"auth\\\":\\\"3263294933dd2762\\\",\\\"followees\\\":{\\\"00000021\\\":true,\\\"00000030\\\":true,\\\"0000003a\\\":true,\\\"0000003c\\\":true,\\\"0000003f\\\":true,\\\"00000041\\\":true,\\\"00000053\\\":true,\\\"00000061\\\":true},\\\"followers\\\":{\\\"0000001a\\\":true,\\\"0000001d\\\":true,\\\"0000001f\\\":true,\\\"0000002d\\\":true,\\\"00000031\\\":true,\\\"00000042\\\":true,\\\"00000044\\\":true,\\\"00000047\\\":true,\\\"00000059\\\":true},\\\"password\\\":\\\"password_81\\\",\\\"posts\\\":[\\\"0001000200000006\\\",\\\"0000407400000007\\\",\\\"0000407400000008\\\",\\\"0001000200000015\\\",\\\"0000c08200000016\\\",\\\"0000c08200000017\\\",\\\"0000407400000019\\\",\\\"000040740000001d\\\",\\\"0000801200000021\\\",\\\"0000407400000022\\\",\\\"0001000200000024\\\",\\\"000080120000003c\\\",\\\"0000801200000045\\\",\\\"0000c08200000046\\\",\\\"0001000200000047\\\"],\\\"username\\\":\\\"testuser_81\\\"}\",\"12527130507037919258\":\"{\\\"auth\\\":\\\"536d3faa9ac834cc\\\",\\\"followees\\\":{\\\"00000021\\\":true,\\\"00000026\\\":true,\\\"00000028\\\":true,\\\"0000003e\\\":true,\\\"00000041\\\":true,\\\"00000050\\\":true,\\\"0000005a\\\":true,\\\"0000005e\\\":true},\\\"followers\\\":{\\\"00000008\\\":true,\\\"0000000e\\\":true,\\\"00000012\\\":true,\\\"00000038\\\":true,\\\"00000040\\\":true,\\\"00000045\\\":true,\\\"0000004b\\\":true,\\\"00000058\\\":true,\\\"0000005a\\\":true},\\\"password\\\":\\\"password_73\\\",\\\"posts\\\":[\\\"0000801200000007\\\",\\\"000040740000001a\\\",\\\"000040740000001d\\\",\\\"000100020000001f\\\",\\\"0000c08200000025\\\",\\\"0000801200000026\\\",\\\"000040740000002b\\\",\\\"0000c08200000032\\\",\\\"0000407400000037\\\",\\\"000040740000003e\\\",\\\"0000407400000043\\\",\\\"0001000200000047\\\"],\\\"username\\\":\\\"testuser_73\\\"}\",\"12534000255689467786\":\"{\\\"auth\\\":\\\"f6709c12705d8db4\\\",\\\"followees\\\":{\\\"00000000\\\":true,\\\"0000000f\\\":true,\\\"00000021\\\":true,\\\"0000003c\\\":true,\\\"00000041\\\":true,\\\"00000054\\\":true,\\\"0000005e\\\":true},\\\"followers\\\":{\\\"00000005\\\":true,\\\"00000019\\\":true,\\\"0000001e\\\":true,\\\"00000031\\\":true,\\\"0000003a\\\":true,\\\"00000043\\\":true,\\\"00000057\\\":true,\\\"0000005f\\\":true},\\\"password\\\":\\\"password_56\\\",\\\"posts\\\":[\\\"0000407400000003\\\",\\\"0000c08200000005\\\",\\\"0000407400000008\\\",\\\"0000c08200000016\\\",\\\"0000c08200000029\\\",\\\"0000c08200000036\\\",\\\"0000c08200000042\\\",\\\"0000801200000044\\\",\\\"0001000200000047\\\"],\\\"username\\\":\\\"testuser_56\\\"}\",\"12638208091276578005\":\"\\\"t\\\"\",\"18446744073709551615\":\"\\\"t\\\"\",\"9689510388238348346\":\"{\\\"body\\\":\\\"De08HZ91PAU4SG4NcFygCYEAj76Ahddv6wTz2GAQmL5HxhetnaIisujbRynTqqDc\\\",\\\"id\\\":\\\"0001000200000047\\\",\\\"userId\\\":\\\"00000021\\\",\\\"userName\\\":\\\"testuser_32\\\"}\"}"
	compressed := CompressData([]byte(data))
	t.Log(len(data), len(compressed))
}

func TestDecompress(t *testing.T) {
	data := []byte{123, 34, 49, 53, 57, 48, 56, 56, 52, 51, 56, 52, 48, 53, 56, 50, 55, 56, 50, 50, 57, 48, 34, 58, 34, 123, 92, 34, 118, 97, 108, 117, 101, 92, 34, 58, 48, 125, 34, 125}
	t.Log(string(data))
}
