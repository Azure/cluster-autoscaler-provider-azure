/*
Copyright (c) Microsoft Corporation.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"sigs.k8s.io/cluster-autoscaler/pkg/config/flags"
)

func TestAKSCompatibilityFlags(t *testing.T) {
	fs := pflag.NewFlagSet("aks", pflag.ContinueOnError)
	autoscalingFlags := &flags.AutoscalingFlags{}
	autoscalingFlags.AddFlags(fs)

	for _, name := range []string{
		"config-path", "enable-force-delete", "enable-dynamic-instance-list", "enable-detailed-cse-message",
	} {
		if fs.Lookup(name) != nil {
			t.Fatalf("%s is now defined by core and should not be registered locally", name)
		}
	}

	configPath := addAKSCompatibilityFlags(fs)

	for _, name := range []string{
		"enable-force-delete", "enable-dynamic-instance-list", "enable-detailed-cse-message",
	} {
		flag := fs.Lookup(name)
		if flag == nil || flag.Value.Type() != "bool" || flag.DefValue != "false" {
			t.Fatalf("%s should be a false-by-default boolean flag: %v", name, flag)
		}

		if !strings.Contains(flag.Usage, "No effect") {
			t.Errorf("%s should say it has no effect", name)
		}
	}

	if *configPath != "" {
		t.Fatalf("config-path default = %q, want empty", *configPath)
	}

	if err := fs.Parse([]string{
		"--config-path=/opt/conf/autoscaler/settings.json",
		"--enable-force-delete",
		"--enable-dynamic-instance-list=true",
		"--enable-detailed-cse-message=false",
		"--cloud-provider=azure",
		"--nodes=0:5:pool",
	}); err != nil {
		t.Fatal(err)
	}

	if *configPath != "/opt/conf/autoscaler/settings.json" {
		t.Errorf("config-path = %q", *configPath)
	}

	for name, want := range map[string]bool{
		"enable-force-delete":          true,
		"enable-dynamic-instance-list": true,
		"enable-detailed-cse-message":  false,
	} {
		if got, err := fs.GetBool(name); err != nil || got != want {
			t.Errorf("%s = %t, %v; want %t", name, got, err, want)
		}
	}
}

func TestAKSCompatibilityFlagsRejectInvalidBool(t *testing.T) {
	for _, name := range []string{
		"enable-force-delete", "enable-dynamic-instance-list", "enable-detailed-cse-message",
	} {
		t.Run(name, func(t *testing.T) {
			fs := pflag.NewFlagSet("aks", pflag.ContinueOnError)
			addAKSCompatibilityFlags(fs)
			if err := fs.Parse([]string{"--" + name + "=invalid"}); err == nil {
				t.Fatal("invalid boolean must not be accepted")
			}
		})
	}
}
