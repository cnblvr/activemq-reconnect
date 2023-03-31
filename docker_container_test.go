package activemq

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const imageName = "rmohr/activemq"

type containerActiveMQ struct {
	t                  *testing.T
	name               string
	id                 string
	portStomp          string
	portFrontend       string
	schedulerSupport   bool
	persistentMessages bool
}

func (c *containerActiveMQ) Name() string         { return c.name }
func (c *containerActiveMQ) ID() string           { return c.id }
func (c *containerActiveMQ) PortStomp() string    { return c.portStomp }
func (c *containerActiveMQ) PortFrontend() string { return c.portFrontend }

func runContainerActiveMQ(t *testing.T, name string, portStomp, portFrontend string) *containerActiveMQ {
	if len(portStomp) == 0 {
		portStomp = "61613"
	}

	cont := &containerActiveMQ{
		t:            t,
		name:         name,
		portStomp:    portStomp,
		portFrontend: portFrontend,
	}

	// docker run -p 61613:61613 -p 8161:8161 --name amq_test -d rmohr/activemq
	runArgs := []string{"run", "-p", cont.portStomp + ":61613"}
	if len(cont.portFrontend) != 0 {
		runArgs = append(runArgs, "-p", cont.portFrontend+":8161")
	}
	if len(name) != 0 {
		runArgs = append(runArgs, "--name", name)
	}
	runArgs = append(runArgs, "-d", imageName)

	contID, ok := runCommand(t, "docker", runArgs...)
	if !ok {
		// try removing the container and attempt again
		cont.Kill()
		cont.Remove()
		contID, ok = runCommand(t, "docker", runArgs...)
		if !ok {
			t.Fatal("failed to run container")
		}
	}
	cont.id = strings.TrimSpace(contID)

	ok = cont.SetSchedulerSupport()
	if !assert.True(t, ok) {
		t.Fatal("failed to set attribute schedulerSupport")
	}

	// remove the container after work
	t.Cleanup(func() {
		cont.Kill()
		cont.Remove()
	})

	return cont
}

func (c *containerActiveMQ) Start() bool {
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	_, ok := runCommand(c.t, "docker", "start", id)
	return ok
}

func (c *containerActiveMQ) Stop() bool {
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	_, ok := runCommand(c.t, "docker", "stop", id)
	return ok
}

func (c *containerActiveMQ) Restart() bool {
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	_, ok := runCommand(c.t, "docker", "restart", id)
	return ok
}

func (c *containerActiveMQ) Kill() bool {
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	_, ok := runCommand(c.t, "docker", "kill", id)
	return ok
}

func (c *containerActiveMQ) Remove() bool {
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	_, ok := runCommand(c.t, "docker", "rm", id)
	return ok
}

func (c *containerActiveMQ) SetSchedulerSupport() bool {
	if !assert.False(c.t, c.schedulerSupport, "the container can't be set schedulerSupport attribute because it has been set") {
		return false
	}
	const replaceCandidate = `<broker xmlns="http://activemq.apache.org/schema/core"`
	ok := c.changeConfiguration("activemq.xml", func(bts []byte) []byte {
		return bytes.Replace(bts, []byte(replaceCandidate), []byte(replaceCandidate+" schedulerSupport=\"true\""), 1)
	})

	if ok {
		c.schedulerSupport = true
	}

	return ok
}

func (c *containerActiveMQ) SetPersistentMessages(persistent bool) bool {
	if !assert.False(c.t, c.persistentMessages, "the container can't be set persistent attribute because it has been set") {
		return false
	}
	const persistentPlace = `<broker xmlns="http://activemq.apache.org/schema/core"`
	const journalPlace = `<kahaDB directory="${activemq.data}/kahadb"`
	ok := c.changeConfiguration("activemq.xml", func(bts []byte) []byte {
		bts = bytes.Replace(bts, []byte(persistentPlace), []byte(fmt.Sprintf(persistentPlace+" persistent=\"%t\"", persistent)), 1)
		bts = bytes.Replace(bts, []byte(journalPlace), []byte(journalPlace+" journalMaxFileLength=\"32mb\""), 1)
		return bts
	})

	if ok {
		c.persistentMessages = true
	}

	return ok
}

// activemq.xml
func (c *containerActiveMQ) changeConfiguration(confName string, fn func(bts []byte) []byte) bool {
	fin, err := os.CreateTemp(os.TempDir(), "")
	if !assert.NoError(c.t, err) {
		return false
	}
	fin.Close()
	defer os.Remove(fin.Name())

	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}

	c.Kill()
	// docker cp -q amq_test:/opt/activemq/conf/activemq.xml /tmp/activemq.xml
	filename := "/opt/activemq/conf/" + confName
	_, ok := runCommand(c.t, "docker", "cp", "-q", id+":"+filename, fin.Name())
	if !assert.True(c.t, ok) {
		return false
	}

	bts, err := os.ReadFile(fin.Name())
	if !assert.NoError(c.t, err) {
		return false
	}

	bts = fn(bts)

	fout, err := os.CreateTemp(os.TempDir(), "")
	if !assert.NoError(c.t, err) {
		return false
	}
	defer os.Remove(fout.Name())
	defer fout.Close()

	if _, err := fout.Write(bts); !assert.NoError(c.t, err) {
		return false
	}
	if err := fout.Sync(); !assert.NoError(c.t, err) {
		return false
	}
	fout.Close()

	_, ok = runCommand(c.t, "docker", "cp", "-q", fout.Name(), id+":"+filename)
	if !assert.True(c.t, ok) {
		return false
	}

	c.Start()
	_, ok = runCommand(c.t, "docker", "exec", "-i", "-u", "root", id, "chown", "activemq:activemq", filename)
	if !assert.True(c.t, ok) {
		return false
	}

	return true
}

func runCommand(t *testing.T, command string, args ...string) (string, bool) {
	cmd := exec.Command(command, args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdReader := io.MultiReader(stdout, stderr)

	id := fmt.Sprintf("%08x", rand.Uint64())
	t.Logf("$ %s\n[%s]", cmd.String(), id)

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	_, _ = stdout, stderr
	btsOutput, err := io.ReadAll(stdReader)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[%s]\n%s", id, string(btsOutput))

	if err := cmd.Wait(); err != nil {
		t.Log(err)
		return "", false
	}
	return string(btsOutput), true
}
