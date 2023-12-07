package lsm

// func TestSSTable(t *testing.T) {
// 	assert := assert.New(t)
// 	m := NewMemTable(math.MaxUint32)
// 	vmap := map[string]string{}

// 	// insert
// 	for i := 0; i < 10000; i++ {
// 		ts := time.Now().UnixNano()
// 		k := strconv.Itoa(int(ts))

// 		vmap[k] = k
// 		m.Put([]byte(k), []byte(k))
// 	}

// 	// dump
// 	err := os.WriteFile("test.sst", EncodeTable(m), 0644)
// 	assert.Nil(err)

// 	// decodeAll
// 	sst, _ := NewSSTable("test.sst")
// 	defer sst.Close()

// 	assert.Nil(sst.loadAllDataBlock())

// 	for k, v := range vmap {
// 		res, cached, err := sst.findKey([]byte(k))
// 		assert.True(cached)
// 		assert.Nil(err)
// 		assert.Equal(v, string(res))
// 	}

// 	// find
// 	for k, v := range vmap {
// 		res, cached, err := sst.findKey([]byte(k))
// 		assert.True(cached)
// 		assert.Nil(err)
// 		assert.Equal(v, string(res))
// 	}

// 	// error find
// 	for i := 0; i < 1000; i++ {
// 		ts := time.Now().UnixNano()
// 		k := strconv.Itoa(int(ts))
// 		res, cached, err := sst.findKey([]byte(k))
// 		assert.False(cached, string(k))
// 		assert.Equal(ErrKeyNotFound, err)
// 		assert.Equal("", string(res))
// 	}

// 	// error open
// 	_, err = NewSSTable("not-exist.sst")
// 	assert.NotNil(err)
// }

// func TestCompact(t *testing.T) {
// 	assert := assert.New(t)

// 	m1 := NewMemTable()
// 	for i := 1000; i < 2000; i++ {
// 		k := []byte(strconv.Itoa(i))
// 		m1.Put(k, k)
// 	}
// 	assert.Nil(os.WriteFile("m1.sst", EncodeTable(m1), 0644))

// 	m2 := NewMemTable()
// 	for i := 3000; i < 4000; i++ {
// 		k := []byte(strconv.Itoa(i))
// 		m2.Put(k, k)
// 	}
// 	assert.Nil(os.WriteFile("m2.sst", EncodeTable(m2), 0644))

// 	// load.
// 	s1, _ := NewSSTable("m1.sst")
// 	s2, _ := NewSSTable("m2.sst")

// 	assert.Equal(string(s1.indexBlock.FirstKey), "1000")
// 	assert.Equal(string(s1.indexBlock.LastKey), "1999")

// 	assert.Equal(string(s2.indexBlock.FirstKey), "3000")
// 	assert.Equal(string(s2.indexBlock.LastKey), "3999")

// 	assert.False(s1.IsOverlap(s2))
// 	assert.True(s1.IsOverlap(s1))

// 	// merge.
// 	s1.Merge(s2)
// 	err := os.WriteFile("m3.sst", EncodeTable(s1.m), 0644)
// 	assert.Nil(err)

// 	s3, err := NewSSTable("m3.sst")
// 	assert.Nil(err)

// 	assert.Equal(string(s3.indexBlock.FirstKey), "1000")
// 	assert.Equal(string(s3.indexBlock.LastKey), "3999")

// 	// find.
// 	{
// 		// the first find should not hit the cache.
// 		k := []byte("1000")
// 		res, cached, err := s3.findKey(k)
// 		assert.False(cached, string(k))
// 		assert.Nil(err)
// 		assert.Equal(string(k), string(res))

// 		// the follow 100 finds should hit the cache.
// 		for i := 1000; i < 1100; i++ {
// 			k := []byte(strconv.Itoa(i))
// 			res, cached, err := s3.findKey(k)
// 			assert.True(cached, string(k))
// 			assert.Nil(err)
// 			assert.Equal(string(k), string(res))
// 		}

// 		// the last key should not hit the cache.
// 		k = []byte("3999")
// 		res, cached, err = s3.findKey(k)
// 		assert.False(cached, string(k))
// 		assert.Nil(err)
// 		assert.Equal(string(k), string(res))
// 	}

// 	// checked data.
// 	for i := 1000; i < 2000; i++ {
// 		k := []byte(strconv.Itoa(i))
// 		res, _, err := s3.findKey(k)
// 		assert.Nil(err)
// 		assert.Equal(string(k), string(res))
// 	}
// 	for i := 3000; i < 4000; i++ {
// 		k := []byte(strconv.Itoa(i))
// 		res, _, err := s3.findKey(k)
// 		assert.Nil(err)
// 		assert.Equal(string(k), string(res))
// 	}
// }
