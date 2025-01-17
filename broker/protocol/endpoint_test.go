package protocol

import (
	gc "gopkg.in/check.v1"
)

type EndpointSuite struct{}

func (s *EndpointSuite) TestValidation(c *gc.C) {
	var cases = []struct {
		ep     Endpoint
		expect string
	}{
		{"http://host:1234/path?query", ""}, // Success.
		{":garbage: :garbage:", "parse .* missing protocol scheme"},
		{"/baz/bing", "not absolute: .*"},
		{"http:///baz/bing", "missing host: .*"},
	}
	for _, tc := range cases {
		if tc.expect == "" {
			c.Check(tc.ep.Validate(), gc.IsNil)
		} else {
			c.Check(tc.ep.Validate(), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *EndpointSuite) TestURLConversion(c *gc.C) {
	var ep Endpoint = "http://host:1234/path?query"
	c.Check(ep.URL().Host, gc.Equals, "host:1234")

	ep = "/baz/bing"
	c.Check(func() { ep.URL() }, gc.PanicMatches, "not absolute: .*")
}

var _ = gc.Suite(&EndpointSuite{})
