package Search::OpenSearch::Federated;

use strict;
use warnings;

our $VERSION = '0.003';

use base 'Search::Tools::Object';
__PACKAGE__->mk_accessors(qw( fields urls total timeout ));

use Carp;
use Data::Dump qw( dump );
use Parallel::Iterator qw( iterate_as_array );
use JSON;
use LWP::UserAgent;
use Scalar::Util qw( blessed );
use Search::Tools::XML;
use Data::Transformer;

# we do not use WWW::OpenSearch because we need to pull out
# some non-standard data from the XML.
# we do use XML::Feed to parse XML responses.
use XML::Simple;
use XML::Feed;

my $OS_NS = 'http://a9.com/-/spec/opensearch/1.1/';

my $XMLer = Search::Tools::XML->new();

my $XML_ESCAPER = Data::Transformer->new(
    normal => sub { local ($_) = shift; $$_ = $XMLer->escape($$_); } );

sub init {
    my $self = shift;
    $self->SUPER::init(@_);
    $self->{fields} ||= [qw( title id author link summary tags modified )];
    return $self;
}

sub search {
    my $self = shift;

    my $urls     = $self->{urls} or croak "no urls defined";
    my $num_urls = scalar @$urls;
    my @done     = iterate_as_array(
        sub {
            $self->_fetch( $_[1] );
        },
        $urls,
    );

    return $self->_aggregate( \@done );

}

sub _aggregate {
    my $self      = shift;
    my $responses = shift;
    my $results   = [];
    my $fields    = $self->fields;
    my $total     = 0;

RESP: for my $resp (@$responses) {

        my $req_uri = $resp->request->uri;
        $self->debug
            and warn sprintf( "response for %s\n", $req_uri );
        if ( $resp->content_type eq 'application/json' ) {
            my $r = decode_json( $resp->content );
            if ( $r->{results} ) {
                push @$results, @{ $r->{results} };
            }
            $total += $r->{total} || 0;
        }
        elsif ( $resp->content_type eq 'application/xml' ) {
            my $xml = $resp->content;

            #warn $xml;
            my $feed = XML::Feed->parse( \$xml );

            if ( !$feed ) {
                warn XML::Feed->errstr;
                next RESP;
            }

            #dump $feed;

            #
            # we must re-escape the XML content since the feed parser
            # and XML::Simple will esacpe values automatically
            #
            my @entries;
            for my $item ( $feed->entries ) {
                my $e = {};
                for my $f (@$fields) {
                    $e->{$f} = $item->$f;
                    if ( blessed( $e->{$f} ) ) {

                        #dump( $e->{$f} );
                        if ( $e->{$f}->isa('XML::Feed::Content') ) {
                            $e->{$f} = $XMLer->escape( $e->{$f}->body );
                        }
                        elsif ( $e->{$f}->isa('DateTime') ) {
                            $e->{$f} = $e->{$f}->epoch;
                        }
                    }
                    else {
                        $e->{$f} = $XMLer->escape( $e->{$f} );
                    }
                }

                #dump $e;
                my $content = $item->content;
                my $fields = XMLin( $content->body, NoAttr => 1 );

                #dump $fields;

                for my $f ( keys %$fields ) {
                    $e->{$f} = $fields->{$f};
                    if ( ref $e->{$f} ) {
                        $XML_ESCAPER->traverse( $e->{$f} );
                    }
                    else {
                        $e->{$f} = $XMLer->escape( $e->{$f} );
                    }
                }

                # massage some field names
                $e->{mtime} = delete $e->{modified};
                $e->{uri}   = delete $e->{id};

                #dump $content;
                #dump $e;
                push @entries, $e;

            }

            my $atom = $feed->{atom};
            $total += $atom->get( $OS_NS, 'totalResults' );

            push @$results, @entries;
        }
        else {
            croak sprintf( "Unsupported response type '%s' for %s\n",
                scalar $resp->content_type, $req_uri );
        }
    }
    $self->{total} = $total;
    return [ sort { $b->{score} <=> $a->{score} } @$results ];
}

sub _fetch {
    my $self = shift;
    my $url  = shift or croak "url required";
    my $ua   = LWP::UserAgent->new();
    $ua->agent('apm-fedsearch');
    $ua->timeout( $self->{timeout} ) if $self->{timeout};

    my $response = $ua->get($url);

    $self->debug and warn "got response for $url: " . $response->status_line;
    return $response;
}

1;

__END__

=head1 NAME

Search::OpenSearch::Federated - aggregate OpenSearch results

=head1 SYNOPSIS

 my $ms = Search::OpenSearch::Federated->new(
    urls    => [
        'http://some-site.org/search?q=foo',
        'http://some-other-site.org/search?q=foo',
    ],
    timeout => 10,  # very generous
 );

 my $results = $ms->search();
 for my $r (@$results) {
     printf("title=%s", $r->title);
     printf("uri=%s",   $r->uri);
     print "\n";
 }

=head1 DESCRIPTION

Search::OpenSearch::Federated is for aggregating multiple OpenSearch responses
into a single result set. Use it as a client for Search::OpenSearch::Engine-powered
servers or for any server that provides OpenSearch-style results.

=head1 METHODS

Search::OpenSearch::Federated isa Search::Tools::Object.

=head2 new( I<args> )

Constructor. I<args> should include key C<urls> with value of
an array reference. Supported I<args> keys are:

=over

=item urls I<arrayref>

=item timeout I<n>

=item fields I<arrayref>

=item debug 0|1

=back

=head2 init

Internal initialization method. Overrides Search::Tools::Object->init.

=head2 search

Execute the search. Returns array ref of results sorted by score.

=head2 fields

Returns fields set in new().

=head2 total

Return total hits.

=head1 COPYRIGHT

Copyright 2013 - American Public Media Group

=head1 AUTHOR

Peter Karman, C<< <karman at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-search-opensearch-federated at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Search-OpenSearch-Federated>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Search::OpenSearch::Federated

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Search-OpenSearch-Federated>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Search-OpenSearch-Federated>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Search-OpenSearch-Federated>

=item * Search CPAN

L<http://search.cpan.org/dist/Search-OpenSearch-Federated/>

=back


=head1 ACKNOWLEDGEMENTS

Thanks to American Public Media and the state of Minnesota for sponsoring the 
development of this module.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut

