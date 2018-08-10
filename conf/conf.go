package conf

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []property `xml:"property"`
}

var confFiles = []string{"core-site.xml", "hdfs-site.xml"}

type hadoopConf map[string]string

func Load(path string) (hadoopConf, error) {
	var conf hadoopConf

	for _, file := range confFiles {
		pList := propertyList{}
		f, err := ioutil.ReadFile(filepath.Join(path, file))
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return conf, err
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			return conf, fmt.Errorf("%s: %s", path, err)
		}

		if conf == nil {
			conf = make(hadoopConf)
		}

		for _, prop := range pList.Property {
			conf[prop.Name] = prop.Value
		}
	}

	return conf, nil
}

func GetClusterName(conf hadoopConf) (clusterName string) {
	return conf["dfs.nameservices"]
}

func GetNameNodes(conf hadoopConf) (NameNodes map[string]string) {
	var (
		nnAlias []string
		rpcKey  string
	)

	clusterName := conf["dfs.nameservices"]
	aliasKey := "dfs.ha.namenodes." + clusterName
	nnAlias = strings.Split(conf[aliasKey], ",")

	NameNodes = make(map[string]string)
	for _, alias := range nnAlias {
		rpcKey = "dfs.namenode.rpc-address." + clusterName + "." + alias
		NameNodes[alias] = conf[rpcKey]
	}

	return
}

func GetZookeeper(conf hadoopConf) (zkList []string) {
	return strings.Split(conf["ha.zookeeper.quorum"], ",")
}
