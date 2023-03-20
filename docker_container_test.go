package activemq

import (
	"fmt"
	"io"
	"math/rand"
	"os/exec"
	"strings"
	"testing"
)

const imageName = "rmohr/activemq"

type containerActiveMQ struct {
	t            *testing.T
	name         string
	id           string
	portStomp    string
	portFrontend string
	isRunning    bool
	isRemoved    bool
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
		cont.isRunning = true
		cont.Kill()
		cont.isRemoved = false
		cont.Remove()
		contID, ok = runCommand(t, "docker", runArgs...)
		if !ok {
			t.Fatal("failed to run container")
		}
	}
	cont.id = strings.TrimSpace(contID)
	cont.isRunning = true
	cont.isRemoved = false

	// remove the container after work
	t.Cleanup(func() {
		cont.Kill()
		cont.Remove()
	})

	return cont
}

func (c *containerActiveMQ) Start() {
	if c.isRemoved {
		c.t.Log("the container can't be started because it has been removed")
		return
	}
	if c.isRunning {
		c.t.Log("the container can't be started because it is already running")
		return
	}
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	if _, ok := runCommand(c.t, "docker", "start", id); ok {
		c.isRunning = true
	}
}

func (c *containerActiveMQ) Stop() {
	if c.isRemoved {
		c.t.Log("the container can't be stopped because it has been removed")
		return
	}
	if !c.isRunning {
		c.t.Log("the container can't be stopped because it is not running")
		return
	}
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	if _, ok := runCommand(c.t, "docker", "stop", id); ok {
		c.isRunning = false
	}
}

func (c *containerActiveMQ) Kill() {
	if c.isRemoved {
		c.t.Log("the container can't be killed because it has been removed")
		return
	}
	if !c.isRunning {
		c.t.Log("the container can't be killed because it is not running")
		return
	}
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	if _, ok := runCommand(c.t, "docker", "kill", id); ok {
		c.isRunning = false
	}
}

func (c *containerActiveMQ) Remove() {
	if c.isRemoved {
		c.t.Log("the container can't be removed because it has already been removed")
		return
	}
	if c.isRunning {
		c.t.Log("the container can't be removed because it is running")
		return
	}
	id := c.id
	if len(c.name) != 0 {
		id = c.name
	}
	if _, ok := runCommand(c.t, "docker", "rm", id); ok {
		c.isRemoved = true
	}
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
